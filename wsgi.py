# wsgi.py
import os, sys, importlib.util
from flask import Flask, redirect, request
from werkzeug.middleware.dispatcher import DispatcherMiddleware

BASE_DIR = os.path.dirname(__file__)

# Deixa os submódulos no sys.path para suportar imports locais
sys.path.insert(0, os.path.join(BASE_DIR, "conferencia_app"))
sys.path.insert(0, os.path.join(BASE_DIR, "pontuacao_app"))

def load_flask_app(folder, candidates=("app.py", "main.py"), var_candidates=("app", "create_app")):
    """Carrega <folder>/<arquivo>.py e retorna a Flask app ('app' ou 'create_app()')."""
    base = os.path.join(BASE_DIR, folder)
    for fname in candidates:
        path = os.path.join(base, fname)
        if os.path.exists(path):
            spec = importlib.util.spec_from_file_location(f"{folder}.{fname[:-3]}", path)
            m = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(m)
            # primeiro tenta variável 'app', depois função 'create_app()'
            if hasattr(m, "app"):
                return getattr(m, "app")
            if hasattr(m, "create_app") and callable(m.create_app):
                return m.create_app()
    raise RuntimeError(f"Não encontrei app em {folder} (tentei {candidates}).")

# Carrega as duas apps
conferencia_app = load_flask_app("conferencia_app")
pontuacao_app   = load_flask_app("pontuacao_app")

# Alias para compatibilidade: /conferencia/... -> /...
# (Se alguém acessar /conferencia/gestao, redireciona para /gestao)
alias = Flask("alias")
@alias.route("/", defaults={"subpath": ""})
@alias.route("/<path:subpath>")
def alias_to_root(subpath):
    qs = ("?" + request.query_string.decode()) if request.query_string else ""
    return redirect("/" + subpath + qs, code=302)

# Monta: conferência na RAIZ; pontuação em /pontuacao; e mantém /conferencia como atalho
app = DispatcherMiddleware(conferencia_app, {
    "/pontuacao":  pontuacao_app,
    "/conferencia": alias,   # opcional: atalho/compat
})
