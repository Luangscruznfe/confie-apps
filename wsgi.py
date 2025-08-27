# wsgi.py
import os, sys, importlib.util
from flask import Flask, redirect
from werkzeug.middleware.dispatcher import DispatcherMiddleware

BASE_DIR = os.path.dirname(__file__)

# >>> Adiciona as pastas dos submódulos no sys.path para suportar imports como:
# from parser_mapa import parse_mapa  (dentro de conferencia_app/app.py)
sys.path.insert(0, os.path.join(BASE_DIR, "conferencia_app"))
sys.path.insert(0, os.path.join(BASE_DIR, "pontuacao_app"))

def load_flask_app(folder, candidates=("app.py","main.py"), var_candidates=("app","create_app")):
    """Carrega <folder>/<arquivo>.py e retorna a Flask app ('app' ou 'create_app()')."""
    base = os.path.join(BASE_DIR, folder)
    for fname in candidates:
        path = os.path.join(base, fname)
        if os.path.exists(path):
            spec = importlib.util.spec_from_file_location(f"{folder}.{fname[:-3]}", path)
            m = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(m)
            # tenta primeiro 'app' (variável), depois 'create_app()'
            if hasattr(m, "app"):
                return getattr(m, "app")
            if hasattr(m, "create_app") and callable(m.create_app):
                return m.create_app()
    raise RuntimeError(f"Não encontrei app em {folder} (tentei {candidates}).")

# carrega as duas apps
conferencia_app = load_flask_app("conferencia_app")
pontuacao_app   = load_flask_app("pontuacao_app")

# app raiz só para redirecionar e healthcheck
root = Flask("root")

@root.route("/")
def index():
    return redirect("/conferencia", code=302)

@root.route("/healthz")
def healthz():
    return ("", 204)

# monta as duas aplicações sob prefixos diferentes
app = DispatcherMiddleware(root, {
    "/conferencia": conferencia_app,
    "/pontuacao":   pontuacao_app,
})
