from flask import Flask
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from conferencia_app.app import app as conferencia_app
from pontuacao_app.app import app as pontuacao_app

# (opcional) isolar sessão da Pontuação
pontuacao_app.config.update(SESSION_COOKIE_NAME='pont_session',
                            SESSION_COOKIE_PATH='/pontuacao')

# health opcional em /_/healthz
health = Flask(__name__)
@health.get("/healthz")
def ok(): return "ok", 200

# Conferência na raiz; Pontuação em /pontuacao
app = DispatcherMiddleware(conferencia_app, {
    "/pontuacao": pontuacao_app,
    "/_": health,
})
