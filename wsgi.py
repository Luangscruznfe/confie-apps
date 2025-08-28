from werkzeug.middleware.dispatcher import DispatcherMiddleware
from flask import Flask, render_template_string
import importlib

# carrega conferencia (arquivo costuma ser conferencia_app/app.py)
conferencia = importlib.import_module("conferencia_app.app").app

# tenta carregar pontuacao por dois nomes comuns
try:
    pontuacao = importlib.import_module("pontuacao_app.app").app
except ModuleNotFoundError:
    pontuacao = importlib.import_module("pontuacao_app.sistema_pontuacao_flask").app

root = Flask(__name__)

@root.route("/", strict_slashes=False)
def index():
    return render_template_string("""
    <!doctype html><html lang="pt-br"><head>
      <meta charset="utf-8"><title>Confie Apps</title>
      <style>
        body{font-family:Arial;padding:40px;background:#0b0f1a;color:#fff}
        .grid{display:grid;gap:16px;grid-template-columns:repeat(auto-fit,minmax(240px,1fr))}
        a.btn{display:block;padding:18px 22px;border-radius:10px;text-decoration:none;text-align:center;background:#1f2937;color:#fff}
        a.btn:hover{background:#374151}
      </style>
    </head><body>
      <h2>Escolha o aplicativo</h2>
      <div class="grid">
        <a class="btn" href="/conferencia/">Conferência de Mercadorias</a>
        <a class="btn" href="/pontuacao/">Pontuação</a>
      </div>
    </body></html>
    """)

app = DispatcherMiddleware(root, {
    "/conferencia": conferencia,
    "/pontuacao": pontuacao,
})
