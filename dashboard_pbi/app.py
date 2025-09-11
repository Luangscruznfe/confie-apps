# dashboard_pbi/app.py --- VERSÃO DE DIAGNÓSTICO

import pandas as pd
import plotly.express as px
from flask import Flask, request, render_template, flash
import os

# Cria a instância da aplicação Flask para o dashboard
app = Flask(__name__)
app.secret_key = 'sua-chave-secreta-aqui-novamente' 

# --- SEÇÃO DE DIAGNÓSTICO DO ARQUIVO CSV ---
CATALOGO_PATH = os.path.join(os.path.dirname(__file__), 'catalogo_produtos.csv')
diagnostico_msg = ""
try:
    # Abre o arquivo e lê apenas a primeira linha (cabeçalho)
    with open(CATALOGO_PATH, 'r', encoding='latin1') as f:
        header_line = f.readline().strip()
        diagnostico_msg = f"CABEÇALHO ENCONTRADO: '{header_line}'. "
        
        # Conta as colunas usando ponto e vírgula como separador
        cols_semicolon = header_line.split(';')
        diagnostico_msg += f"// Colunas com separador ';': {len(cols_semicolon)}. "
        
        # Conta as colunas usando vírgula como separador
        cols_comma = header_line.split(',')
        diagnostico_msg += f"// Colunas com separador ',': {len(cols_comma)}."

except Exception as e:
    # Se der erro ao ler, também nos informa
    diagnostico_msg = f"ERRO AO TENTAR ABRIR O ARQUIVO DE CATÁLOGO PARA DIAGNÓSTICO: {e}"

# O resto do código é desativado temporariamente para não causar erros
catalogo_df = None


@app.route('/', methods=['GET', 'POST'])
def pagina_upload():
    # A CADA VEZ QUE A PÁGINA É CARREGADA, ELA MOSTRARÁ A MENSAGEM DE DIAGNÓSTICO
    flash(f"INFO DE DIAGNÓSTICO DO SERVIDOR: {diagnostico_msg}")

    # A lógica de processamento de arquivo fica desativada por enquanto
    if request.method == 'POST':
        flash("Modo de diagnóstico ativo. O processamento de arquivos está desativado.")
        return render_template('upload.html')

    return render_template('upload.html')