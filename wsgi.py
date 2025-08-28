# wsgi.py
from flask import Flask
from werkzeug.middleware.dispatcher import DispatcherMiddleware

# importa os dois apps (cada app.py expõe uma variável chamada "app")
from conferencia_app.app import app as conferencia_app
from pontuacao_app.app import app as pontuacao_app

# app raiz apenas para health check
root = Flask(__name__)

@root.get("/")
def health():
    return "ok", 200

# monta as duas aplicações em um serviço
app = DispatcherMiddleware(root, {
    "/conferencia": conferencia_app,
    "/pontuacao":   pontuacao_app,
})
