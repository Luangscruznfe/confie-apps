# wsgi.py
import os, importlib.util
from flask import Flask, redirect
from werkzeug.middleware.dispatcher import DispatcherMiddleware

def load_flask_app(folder, candidates=("app.py","main.py"), var_candidates=("app","create_app")):
    base = os.path.join(os.path.dirname(__file__), folder)
    for fname in candidates:
        path = os.path.join(base, fname)
        if os.path.exists(path):
            spec = importlib.util.spec_from_file_location(f"{folder}.{fname[:-3]}", path)
            m = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(m)
            for v in var_candidates:
                obj = getattr(m, v, None)
                if callable(obj) and v == "create_app":
                    return obj()
                if obj is not None and v == "app":
                    return obj
    raise RuntimeError(f"Não encontrei app em {folder} (tentei {candidates} com {var_candidates}).")

conferencia_app = load_flask_app("conferencia_app")
pontuacao_app   = load_flask_app("pontuacao_app")

root = Flask("root")

@root.route("/")
def index():
    return redirect("/conferencia", code=302)

@root.route("/healthz")
def healthz():
    return ("", 204)

app = DispatcherMiddleware(root, {
    "/conferencia": conferencia_app,
    "/pontuacao":   pontuacao_app,
})
