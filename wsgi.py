# =======================================================
# ALTERAÇÃO AQUI: Adicione estas duas linhas no topo
# =======================================================
from dotenv import load_dotenv
load_dotenv()
# =======================================================

from flask import Flask
from werkzeug.middleware.dispatcher import DispatcherMiddleware

# Importa as suas aplicações existentes
from conferencia_app.app import app as conferencia_app
from pontuacao_app.app import app as pontuacao_app

# Importa a nova aplicação do dashboard que criamos
from dashboard_app.app import app as dashboard_app

# (opcional) isolar sessão da Pontuação
pontuacao_app.config.update(
    SESSION_COOKIE_NAME='pont_session',
    SESSION_COOKIE_PATH='/pontuacao'
)

# health opcional em /_/healthz
health = Flask(__name__)
@health.get("/healthz")
def ok(): return "ok", 200

# Middleware para rotear as requisições:
# A aplicação de Conferência responde na rota principal "/"
# As outras aplicações respondem em sub-rotas
app = DispatcherMiddleware(conferencia_app, {
    "/pontuacao": pontuacao_app,
    "/dashboard": dashboard_app,
    "/_": health,
})

