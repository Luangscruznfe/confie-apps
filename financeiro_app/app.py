# =====================================================================
# financeiro_app/app.py - O "SUPER-ARQUIVO" HÍBRIDO
# ATUALIZADO COM TABELA AG-GRID
# =====================================================================

import os
import pandas as pd
from flask import Flask, render_template, request, flash, redirect, url_for, jsonify, abort
from flask_sqlalchemy import SQLAlchemy
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import func, cast, Date, case, extract, distinct, or_
import calendar
import statistics

# Imports do Dash
import dash
from dash import dcc, html, Input, Output, State
from dash.exceptions import PreventUpdate
import plotly.graph_objects as go
import dash_bootstrap_components as dbc
import dash_ag_grid as dag  # <-- 1. NOVO IMPORT
import base64
import io

# --- 1. INICIALIZAÇÃO DO FLASK E BANCO DE DADOS (DO SEU APP ANTIGO) ---

load_dotenv()

# Inicializa o Servidor Flask PRINCIPAL
server = Flask(__name__, template_folder='templates')

# Configurações do Flask e SQLAlchemy
server.config['SECRET_KEY'] = os.environ.get('SECRET_KEY')
server.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL')
server.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(server)
server.jinja_env.globals.update(case=case, extract=extract)

# --- 2. MODELOS DO BANCO DE DADOS (DO SEU APP ANTIGO) ---
class UploadBatch(db.Model):
    id = db.Column(db.Integer, primary_key=True); data_upload = db.Column(db.DateTime, default=datetime.utcnow); nome_arquivo = db.Column(db.String(200))
    perfis = db.relationship('PerfilCliente', back_populates='batch', lazy='dynamic', cascade="all, delete-orphan")

class PerfilCliente(db.Model):
    id = db.Column(db.Integer, primary_key=True); codigo_cliente = db.Column(db.String, nullable=False); nome_cliente = db.Column(db.String, nullable=False)
    dias_atraso_medio = db.Column(db.Float, default=0); pontualidade = db.Column(db.Float, default=0); titulos_atrasados = db.Column(db.Integer, default=0)
    total_titulos = db.Column(db.Integer, default=0); valor_total_pago = db.Column(db.Float, default=0); risco = db.Column(db.String(10), default='Médio')
    data_pagto = db.Column(db.Date); valor_titulo_pago = db.Column(db.Float, default=0); dias_atraso_titulo = db.Column(db.Integer, default=0)
    vendedor = db.Column(db.String(100)); batch_id = db.Column(db.Integer, db.ForeignKey('upload_batch.id')); batch = db.relationship('UploadBatch', back_populates='perfis')

@server.cli.command("init-db")
def init_db_command(): # <-- Corrigido (tinha init-db_command)
    with server.app_context(): db.create_all()
    print("Banco de dados inicializado/atualizado.")

# --- 3. ROTAS FLASK (DO SEU APP ANTIGO - "ANÁLISE DE LIQUIDADOS") ---
# (Oculto para economizar espaço, este bloco é idêntico ao anterior)
# ... (rotas /liquidados, /tendencias, /historico, /limpar_base) ...
@server.route("/liquidados", methods=["GET", "POST"])
def index_liquidados():
    if request.method == 'POST':
        # --- Lógica do POST para Upload ---
        if 'file' not in request.files or request.files['file'].filename == '': flash('Nenhum arquivo selecionado', 'error'); return redirect(url_for('index_liquidados'))
        file = request.files['file']
        if file and file.filename.endswith('.xlsx'):
            try:
                df = pd.read_excel(file); df.columns = df.columns.str.strip()
                if 'VENDEDOR' not in df.columns: flash("Coluna 'VENDEDOR' não encontrada.", "error"); return redirect(url_for('index_liquidados'))
                df['VENDEDOR'] = df['VENDEDOR'].fillna('N/A')
                df['VENCTO'] = pd.to_datetime(df['VENCTO'], dayfirst=True, errors='coerce')
                df['DT. PAGTO'] = pd.to_datetime(df['DT. PAGTO'], dayfirst=True, errors='coerce')
                df.dropna(subset=['VENCTO', 'DT. PAGTO'], inplace=True)
                try:
                    if 'VL. PAGO' in df.columns and df['VL. PAGO'].dtype == 'object': df['VL. PAGO'] = df['VL. PAGO'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                    df['VL. PAGO'] = pd.to_numeric(df['VL. PAGO'], errors='coerce').fillna(0)
                except Exception as e_parse: df['VL. PAGO'] = 0
                df['DIAS_ATRASO_TITULO'] = (df['DT. PAGTO'] - df['VENCTO']).dt.days

                perfil_clientes_df = df.groupby(['CÓDIGO', 'NOME']).agg( DIAS_ATRASO_MEDIO_GERAL=('DIAS_ATRASO_TITULO', lambda x: x[x > 0].mean()), TITULOS_ATRASADOS_GERAL=('DIAS_ATRASO_TITULO', lambda x: (x > 0).sum()), TITULOS_EM_DIA_GERAL=('DIAS_ATRASO_TITULO', lambda x: (x <= 0).sum()), VALOR_TOTAL_PAGO_GERAL=('VL. PAGO', 'sum') ).reset_index()
                perfil_clientes_df['TOTAL_TITULOS_GERAL'] = perfil_clientes_df['TITULOS_ATRASADOS_GERAL'] + perfil_clientes_df['TITULOS_EM_DIA_GERAL']
                perfil_clientes_df['PONTUALIDADE_GERAL'] = perfil_clientes_df.apply(lambda row: (row['TITULOS_EM_DIA_GERAL'] / row['TOTAL_TITULOS_GERAL']) * 100 if row['TOTAL_TITULOS_GERAL'] > 0 else 0, axis=1)
                perfil_clientes_df['DIAS_ATRASO_MEDIO_GERAL'] = perfil_clientes_df['DIAS_ATRASO_MEDIO_GERAL'].fillna(0)
                def calcular_risco(row): atraso = row['DIAS_ATRASO_MEDIO_GERAL']; pontualidade = row['PONTUALIDADE_GERAL']; return 'Baixo' if atraso <= 5 and pontualidade > 80 else ('Alto' if atraso > 15 or pontualidade < 50 else 'Médio')
                perfil_clientes_df['RISCO_GERAL'] = perfil_clientes_df.apply(calcular_risco, axis=1)

                novo_batch = UploadBatch(nome_arquivo=file.filename); db.session.add(novo_batch); db.session.flush()
                perfil_clientes_df['CÓDIGO'] = perfil_clientes_df['CÓDIGO'].astype(str)
                perfis_dict = perfil_clientes_df.set_index('CÓDIGO').to_dict('index')

                registros_para_salvar = []
                for index, row in df.iterrows():
                    codigo_cliente_str = str(row['CÓDIGO']); codigo_lookup = codigo_cliente_str
                    perfil_agregado = perfis_dict.get(codigo_lookup)
                    if perfil_agregado:
                        novo_perfil = PerfilCliente( codigo_cliente=codigo_cliente_str, nome_cliente=row['NOME'], dias_atraso_medio=perfil_agregado['DIAS_ATRASO_MEDIO_GERAL'], pontualidade=perfil_agregado['PONTUALIDADE_GERAL'], titulos_atrasados=perfil_agregado['TITULOS_ATRASADOS_GERAL'], total_titulos=perfil_agregado['TOTAL_TITULOS_GERAL'], valor_total_pago=perfil_agregado['VALOR_TOTAL_PAGO_GERAL'], risco=perfil_agregado['RISCO_GERAL'], data_pagto=row['DT. PAGTO'].date(), valor_titulo_pago=row['VL. PAGO'], dias_atraso_titulo=row['DIAS_ATRASO_TITULO'], vendedor=row['VENDEDOR'], batch_id=novo_batch.id )
                        registros_para_salvar.append(novo_perfil)
                if registros_para_salvar: db.session.bulk_save_objects(registros_para_salvar); db.session.commit(); flash('Base de dados atualizada com sucesso!', 'success')
                else: db.session.rollback(); flash('Nenhum registro válido encontrado para salvar.', 'warning')
            except KeyError as e: db.session.rollback(); flash(f"Erro: Coluna '{e}' não encontrada.", 'error')
            except Exception as e: db.session.rollback(); print(f"ERRO UPLOAD: {type(e).__name__}: {e}"); flash(f'Erro inesperado: {e}', 'error')
            return redirect(url_for('index_liquidados'))
        else: flash('Arquivo inválido.', 'error'); return redirect(url_for('index_liquidados'))
    latest_batch = UploadBatch.query.order_by(UploadBatch.data_upload.desc()).first()
    dados_clientes = []
    kpis = {"total_clientes": 0, "pontualidade_media": 0, "valor_total": 0, "media_geral_atraso": 0, "perc_receita_em_risco": 0}
    if latest_batch:
        clientes_do_lote = PerfilCliente.query.filter_by(batch_id=latest_batch.id).all()
        clientes_agrupados = {}
        for c in clientes_do_lote:
            if c.codigo_cliente not in clientes_agrupados:
                clientes_agrupados[c.codigo_cliente] = {
                    'CÓDIGO': c.codigo_cliente, 'NOME': c.nome_cliente, 'DIAS_ATRASO_MEDIO': c.dias_atraso_medio,
                    'PONTUALIDADE': c.pontualidade, 'RISCO': c.risco, 'TITULOS_ATRASADOS': c.titulos_atrasados,
                    'TOTAL_TITULOS': c.total_titulos, 'VALOR_TOTAL_PAGO': 0.0,
                    'VALOR_TOTAL_PAGO_DISPLAY': c.valor_total_pago
                }
            clientes_agrupados[c.codigo_cliente]['VALOR_TOTAL_PAGO'] += c.valor_titulo_pago
        dados_clientes = sorted(list(clientes_agrupados.values()), key=lambda x: x['DIAS_ATRASO_MEDIO'], reverse=True)
        if dados_clientes:
            total_clientes = len(dados_clientes); pontualidade_media = sum(c['PONTUALIDADE'] for c in dados_clientes) / total_clientes if total_clientes > 0 else 0; valor_total = sum(c['VALOR_TOTAL_PAGO'] for c in dados_clientes)
            soma_atrasos_ponderada = sum(c['DIAS_ATRASO_MEDIO'] * c['TITULOS_ATRASADOS'] for c in dados_clientes if c['TOTAL_TITULOS'] > 0); total_titulos_atrasados_geral = sum(c['TITULOS_ATRASADOS'] for c in dados_clientes); media_geral_atraso = soma_atrasos_ponderada / total_titulos_atrasados_geral if total_titulos_atrasados_geral > 0 else 0
            valor_em_risco = sum(c['VALOR_TOTAL_PAGO_DISPLAY'] for c in dados_clientes if c['RISCO'] == 'Alto'); perc_receita_em_risco = (valor_em_risco / valor_total) * 100 if valor_total > 0 else 0
            kpis = { "total_clientes": total_clientes, "pontualidade_media": pontualidade_media, "valor_total": valor_total, "media_geral_atraso": media_geral_atraso, "perc_receita_em_risco": perc_receita_em_risco }
    return render_template('dashboard_financeiro.html', clientes=dados_clientes, kpis=kpis)

@server.route("/tendencias")
def tendencias():
    selected_month_str = request.args.get('mes')
    query_base = db.session.query( PerfilCliente )
    dados_evolucao_final = []
    dados_risco = {'Baixo': 0, 'Médio': 0, 'Alto': 0}
    dados_vendedor_atraso_raw = []
    is_monthly_view = False
    base_query_for_stats = PerfilCliente.query
    if selected_month_str:
        is_monthly_view = True
        try:
            year, month = map(int, selected_month_str.split('-')); start_date = datetime(year, month, 1); last_day = calendar.monthrange(year, month)[1]; end_date = datetime(year, month, last_day)
            query_filtrada_evolucao = query_base.filter( PerfilCliente.data_pagto >= start_date, PerfilCliente.data_pagto <= end_date )
            query_filtrada_stats = base_query_for_stats.filter( PerfilCliente.data_pagto >= start_date, PerfilCliente.data_pagto <= end_date )
            stmt = query_filtrada_evolucao.statement.with_only_columns( func.avg(PerfilCliente.pontualidade).label('pontualidade_media'), func.avg(case((PerfilCliente.dias_atraso_titulo > 0, PerfilCliente.dias_atraso_titulo))).label('atraso_medio_titulos_positivos'), func.sum(PerfilCliente.valor_titulo_pago).label('valor_total_mes'), func.sum(case((PerfilCliente.risco == 'Alto', PerfilCliente.valor_titulo_pago), else_=0)).label('valor_risco_mes'), func.sum(case((PerfilCliente.dias_atraso_titulo > 0, 1), else_=0)).label('contagem_titulos_atrasados_mes') )
            dados_mes = db.session.execute(stmt).first()
            if dados_mes and dados_mes.valor_total_mes is not None: dados_evolucao_final = [{'mes_pagto': start_date, **dados_mes._asdict()}]
            risco_mes_query = query_filtrada_stats.group_by(PerfilCliente.risco).with_entities( PerfilCliente.risco, func.count(distinct(PerfilCliente.codigo_cliente)).label('contagem') ).all()
            risco_counts = {r.risco: r.contagem for r in risco_mes_query}; dados_risco.update(risco_counts)
            dados_vendedor_atraso_raw = query_filtrada_stats.filter(PerfilCliente.dias_atraso_titulo > 0)\
                                        .group_by(PerfilCliente.vendedor)\
                                        .with_entities(PerfilCliente.vendedor, func.count().label('contagem_atrasados'))\
                                        .order_by(func.count().desc()).all()
        except Exception as e: flash(f"Erro ao buscar dados do mês: {e}", "error"); print(f"ERRO /tendencias MENSAL: {e}")
    else:
        is_monthly_view = False
        try:
            dados_evolucao_mensal = query_base.group_by(func.date_trunc('month', PerfilCliente.data_pagto)).with_entities( func.date_trunc('month', PerfilCliente.data_pagto).label('mes_pagto'), func.avg(PerfilCliente.pontualidade).label('pontualidade_media'), func.avg(case((PerfilCliente.dias_atraso_titulo > 0, PerfilCliente.dias_atraso_titulo))).label('atraso_medio_titulos_positivos'), func.sum(PerfilCliente.valor_titulo_pago).label('valor_total_mes'), func.sum(case((PerfilCliente.risco == 'Alto', PerfilCliente.valor_titulo_pago), else_=0)).label('valor_risco_mes'), func.sum(case((PerfilCliente.dias_atraso_titulo > 0, 1), else_=0)).label('contagem_titulos_atrasados_mes') ).order_by('mes_pagto').all()
            dados_evolucao_final = [row._asdict() for row in dados_evolucao_mensal if row.mes_pagto is not None]
            risco_geral_query = base_query_for_stats.group_by(PerfilCliente.risco).with_entities( PerfilCliente.risco, func.count(distinct(PerfilCliente.codigo_cliente)).label('contagem') ).all()
            risco_counts = {r.risco: r.contagem for r in risco_geral_query}; dados_risco.update(risco_counts)
            dados_vendedor_atraso_raw = base_query_for_stats.filter(PerfilCliente.dias_atraso_titulo > 0)\
                                        .group_by(PerfilCliente.vendedor)\
                                        .with_entities(PerfilCliente.vendedor, func.count().label('contagem_atrasados'))\
                                        .order_by(func.count().desc()).all()
        except Exception as e: flash(f"Erro ao buscar dados gerais: {e}", "error"); print(f"ERRO /tendencias GERAL: {e}")
    labels = [d['mes_pagto'].strftime('%Y-%m') for d in dados_evolucao_final if d.get('mes_pagto')]
    valores_pontualidade = [round(float(d.get('pontualidade_media', 0) or 0), 2) for d in dados_evolucao_final]
    valores_atraso_medio_titulos = [round(float(d.get('atraso_medio_titulos_positivos', 0) or 0), 1) for d in dados_evolucao_final]
    valores_contagem_atrasados = [int(d.get('contagem_titulos_atrasados_mes', 0) or 0) for d in dados_evolucao_final]
    valores_receita = [round(float(d.get('valor_total_mes', 0) or 0), 2) for d in dados_evolucao_final]
    valores_receita_risco = [round(float(d.get('valor_risco_mes', 0) or 0), 2) for d in dados_evolucao_final]
    labels_risco = list(dados_risco.keys()); valores_risco = list(dados_risco.values())
    labels_vendedor = []; valores_vendedor_atrasados = []
    for d in dados_vendedor_atraso_raw:
        primeiro_nome = str(d.vendedor).split(' ')[0] if d.vendedor else 'N/A'
        labels_vendedor.append(primeiro_nome)
        valores_vendedor_atrasados.append(int(d.contagem_atrasados or 0))

    return render_template("tendencias.html",
                           labels=labels, valores_pontualidade=valores_pontualidade,
                           valores_atraso_medio_titulos=valores_atraso_medio_titulos,
                           valores_contagem_atrasados=valores_contagem_atrasados,
                           labels_risco=labels_risco, valores_risco=valores_risco,
                           valores_receita=valores_receita, valores_receita_risco=valores_receita_risco,
                           labels_vendedor=labels_vendedor,
                           valores_vendedor_atrasados=valores_vendedor_atrasados,
                           selected_month=selected_month_str, is_monthly_view=is_monthly_view)

@server.route("/historico", methods=["GET"])
def historico_cliente():
    busca_codigo = request.args.get('busca_codigo', '').strip()
    busca_nome = request.args.get('busca_nome', '').strip()
    cliente_encontrado = None; historico_cliente = []; labels_atraso = []; valores_atraso = []; nome_cliente = ""; codigo_cliente_encontrado = ""
    if busca_codigo or busca_nome:
        try:
            query = PerfilCliente.query
            filtros = []
            if busca_codigo: filtros.append(PerfilCliente.codigo_cliente.ilike(f"%{busca_codigo}%"))
            if busca_nome: filtros.append(PerfilCliente.nome_cliente.ilike(f"%{busca_nome}%"))
            if filtros: query = query.filter(*filtros)
            cliente_encontrado = query.first()
            if cliente_encontrado:
                codigo_cliente_encontrado = cliente_encontrado.codigo_cliente; nome_cliente = cliente_encontrado.nome_cliente
                historico_agrupado = db.session.query( func.date_trunc('month', PerfilCliente.data_pagto).label('mes_pagto'), func.avg(case((PerfilCliente.dias_atraso_titulo > 0, PerfilCliente.dias_atraso_titulo))).label('atraso_medio_titulos_positivos') ).filter( PerfilCliente.codigo_cliente == codigo_cliente_encontrado, PerfilCliente.data_pagto != None ).group_by('mes_pagto').order_by('mes_pagto').all()
                if historico_agrupado: labels_atraso = [h.mes_pagto.strftime('%Y-%m') for h in historico_agrupado]; valores_atraso = [round(float(h.atraso_medio_titulos_positivos or 0), 1) for h in historico_agrupado]
                else: flash(f"Nenhum histórico de pagamento encontrado para '{nome_cliente}'.", "info")
            else: flash(f"Nenhum cliente encontrado.", "error")
        except Exception as e: print(f"ERRO /historico: {e}"); flash("Erro durante a busca.", "error")
    return render_template('historico_cliente.html', busca_codigo=busca_codigo, busca_nome=busca_nome, cliente=cliente_encontrado, nome_cliente=nome_cliente, codigo_cliente=codigo_cliente_encontrado, historico=historico_cliente, labels_atraso=labels_atraso, valores_atraso=valores_atraso)

@server.route("/limpar_base", methods=["POST"])
def limpar_base():
    try:
        num_batches_deleted = db.session.query(UploadBatch).delete()
        db.session.commit()
        flash(f"Base de dados limpa com sucesso! {num_batches_deleted} lotes e seus perfis associados foram removidos.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Erro ao limpar base: {str(e)}", "danger")
    return redirect(url_for("index_liquidados"))


# --- 4. INICIALIZAÇÃO DO APP DASH (HOME & CONTAS A RECEBER) ---

external_stylesheets = [
    dbc.themes.FLATLY,
    "https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap",
    "https://use.fontawesome.com/releases/v6.4.2/css/all.css"
]

app = dash.Dash(
    __name__,
    server=server,
    requests_pathname_prefix='/financeiro/',
    suppress_callback_exceptions=True,
    external_stylesheets=external_stylesheets
)

# --- 5. LÓGICA DE PROCESSAMENTO DE DADOS (CONTAS A RECEBER) ---
# Esta função será usada pelo carregamento estático E pelo upload

def categorizar_atraso(dias):
    if pd.isnull(dias) or dias < 0:
        return "A Vencer"
    elif 0 <= dias <= 15:
        return "0-15 dias"
    elif 16 <= dias <= 30:
        return "16-30 dias"
    elif 31 <= dias <= 60:
        return "31-60 dias"
    else:
        return "> 60 dias"

def process_cr_data(df):
    """Processa o DataFrame de Contas a Receber e retorna os dados para o gráfico e tabela."""
    
    # --- NOVO: Bloco de Padronização de Colunas ---
    # Renomeia as colunas do arquivo para os nomes que o app espera
    
    # Mapeia 'VCTO ORI' para 'VENCTO'
    if 'VCTO ORI' in df.columns:
        df = df.rename(columns={'VCTO ORI': 'VENCTO'})
    
    # Mapeia 'NOME' para 'CLIENTE'
    if 'NOME' in df.columns:
        df = df.rename(columns={'NOME': 'CLIENTE'})
        
    # Se, depois de tudo, as colunas essenciais não existirem, lança um erro claro
    if 'VENCTO' not in df.columns:
        raise KeyError("Coluna 'VENCTO' ou 'VCTO ORI' não foi encontrada no arquivo.")
    if 'CLIENTE' not in df.columns:
        raise KeyError("Coluna 'CLIENTE' ou 'NOME' não foi encontrada no arquivo.")
    if 'VALOR' not in df.columns:
        raise KeyError("Coluna 'VALOR' não foi encontrada no arquivo.")
    # --- Fim do Bloco de Padronização ---

    # 1. Tratar Colunas
    df['VALOR'] = pd.to_numeric(df['VALOR'], errors='coerce')
    
    # Converte a coluna 'VENCTO' (que agora sabemos que existe) para data
    df['VENCTO'] = pd.to_datetime(df['VENCTO'], errors='coerce')
    
    # Remove linhas que não têm data de vencimento ou valor
    df.dropna(subset=['VENCTO', 'VALOR'], inplace=True)

    # 2. Engenharia de Features
    hoje = datetime.now()
    
    # Este arquivo NÃO tem a coluna 'DIAS', então SEMPRE calculamos
    df['DIAS_ATRASO'] = (hoje - df['VENCTO']).dt.days
    df['DIAS_ATRASO'] = pd.to_numeric(df['DIAS_ATRASO'], errors='coerce')

    # Aplica a função (que já existe no topo do script)
    df['FAIXA_ATRASO'] = df['DIAS_ATRASO'].apply(categorizar_atraso)
    
    # Arredonda o valor para 2 casas decimais para exibição
    df['VALOR'] = df['VALOR'].round(2)

    # 3. Preparar Gráfico Aging List (Idêntico ao anterior)
    aging_data = df.groupby('FAIXA_ATRASO')['VALOR'].sum().reset_index()
    ordem_faixas = ["A Vencer", "0-15 dias", "16-30 dias", "31-60 dias", "> 60 dias"]
    aging_data['FAIXA_ATRASO'] = pd.Categorical(aging_data['FAIXA_ATRASO'], categories=ordem_faixas, ordered=True)
    aging_data = aging_data.sort_values('FAIXA_ATRASO')
    
    fig_aging = go.Figure(data=[
        go.Bar(
            x=aging_data['FAIXA_ATRASO'], 
            y=aging_data['VALOR'],
            text=aging_data['VALOR'].apply(lambda x: f'R$ {x:,.2f}'),
            textposition='auto'
        )
    ])
    fig_aging.update_layout(
        title='Valor Total a Receber por Faixa de Atraso (Aging List)',
        xaxis_title='Faixa de Atraso',
        yaxis_title='Valor Total (R$)',
        yaxis=dict(tickformat=',.2f')
    )
    
    # 4. Preparar Dados da Tabela
    # Seleciona as colunas que sabemos que existem
    colunas_presentes = ['CLIENTE', 'VENDEDOR', 'TÍTULO', 'VENCTO', 'VALOR', 'DIAS_ATRASO']
    # Filtra caso alguma coluna opcional (como VENDEDOR ou TÍTULO) não exista
    colunas_para_tabela = [col for col in colunas_presentes if col in df.columns]
    
    df_tabela = df[colunas_para_tabela]
    
    if 'VENCTO' in df_tabela.columns:
        df_tabela['VENCTO'] = df_tabela['VENCTO'].dt.strftime('%d/%m/%Y') # Formata a data
    
    rowData = df_tabela.to_dict('records')
    
    # Definições das colunas da tabela AG Grid
    columnDefs = [
        {"headerName": "Cliente", "field": "CLIENTE", "sortable": True, "filter": True, "pinned": "left", "width": 250},
        {"headerName": "Vendedor", "field": "VENDEDOR", "sortable": True, "filter": True, "width": 150},
        {"headerName": "Título", "field": "TÍTULO", "sortable": True, "filter": True, "width": 120},
        {"headerName": "Vencimento", "field": "VENCTO", "sortable": True, "filter": "agDateColumnFilter", "width": 140},
        {"headerName": "Dias Atraso", "field": "DIAS_ATRASO", "sortable": True, "filter": "agNumberColumnFilter", "width": 130},
        {
            "headerName": "Valor (R$)", 
            "field": "VALOR", 
            "sortable": True, 
            "filter": "agNumberColumnFilter",
            "width": 150,
            "valueFormatter": {"function": "d3.format(',.2f')"} # Formata para R$
        },
    ]
    
    return fig_aging, rowData, columnDefs

# --- 6. CARREGAMENTO DE DADOS (DASH - INICIAL) ---
# Tenta carregar o arquivo estático ao iniciar
try:
    df_cr_static = pd.read_csv('contas a receber.xlsx - Planilha1.csv')
    fig_aging, grid_rowData, grid_columnDefs = process_cr_data(df_cr_static)
    cr_data_message = f"Dados estáticos carregados com sucesso ({len(grid_rowData)} linhas)."
    
except FileNotFoundError:
    fig_aging = go.Figure()
    grid_rowData = []
    grid_columnDefs = []
    cr_data_message = "Erro: Arquivo 'contas a receber.xlsx - Planilha1.csv' não encontrado. Use o botão de upload."
except Exception as e:
    fig_aging = go.Figure()
    grid_rowData = []
    grid_columnDefs = []
    cr_data_message = f"Erro ao processar arquivo estático: {e}"


# --- 7. LAYOUTS DAS PÁGINAS DASH ---

# Layout da Home (Página principal /financeiro/)
layout_home = dbc.Container([
    html.H2("Home Financeiro - Confie", className="page-title"),
    html.P("Visualize o panorama financeiro da empresa e acesse os módulos de análise.", className="page-subtitle"),
    
    dbc.Row([
        # Card 1: Contas a Receber (Dash)
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.Div(html.I(className="fa-solid fa-money-bill-wave icon-cr"), className="icon-container"),
                html.H4("Contas a Receber", className="card-title"),
                html.P("Análise de títulos em aberto, aging list e top devedores.", className="card-text"),
                dcc.Link(
                    dbc.Button("Acessar Módulo", color="success", className="w-100"), 
                    href='/financeiro/contas-a-receber',
                    style={'textDecoration': 'none'}
                )
            ])
        ], className="nav-card"), md=6, lg=4, className="mb-4"),

        # Card 2: Análise de Liquidados (Flask)
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.Div(html.I(className="fa-solid fa-chart-line icon-liq"), className="icon-container"),
                html.H4("Análise de Liquidados", className="card-title"),
                html.P("Perfil de pagamento de clientes, histórico e PMR (do BD).", className="card-text"),
                html.A(
                    dbc.Button("Acessar Módulo", color="primary", className="w-100"), 
                    href='/financeiro/liquidados', 
                    style={'textDecoration': 'none'}
                )
            ])
        ], className="nav-card"), md=6, lg=4, className="mb-4"),
        
        # Card 3: Placeholder
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.Div(html.I(className="fa-solid fa-credit-card icon-cp"), className="icon-container"),
                html.H4("Contas a Pagar (Em Breve)", className="card-title"),
                html.P("Gestão e previsão de pagamentos futuros.", className="card-text"),
                dbc.Button("Acessar Módulo", color="secondary", disabled=True, className="w-100")
            ])
        ], className="nav-card"), md=6, lg=4, className="mb-4")

    ], justify="center")
], fluid=True, style={"padding": "0 2rem"})

# Layout de Contas a Receber (/financeiro/contas-a-receber)
layout_contas_a_receber = dbc.Container([
    dbc.Container([
        html.H2("Dashboard de Contas a Receber", className="page-title"),
        
        dcc.Link(
            dbc.Button("Voltar para Home", color="secondary"), 
            href="/financeiro/",
            style={'textDecoration': 'none'},
            className="btn-voltar"
        ),
        
        dcc.Upload(
            id='cr-upload-component',
            children=dbc.Button(
                [
                    html.I(className="fa-solid fa-upload me-2"),
                    "Carregar Planilha (.xlsx)"
                ],
                color="primary",
                className="w-100",
                outline=True
            ),
            style={'margin': '10px 0'},
            multiple=False
        ),
        
        html.Div(id='cr-upload-status', children=cr_data_message),
        
        html.Hr(),
        
        # GRÁFICO
        dbc.Row([
            dbc.Col(dcc.Graph(id='graph-aging-list', figure=fig_aging))
        ]),
        
        html.Hr(style={'marginTop': '2rem', 'marginBottom': '2rem'}),
        
        # 3. NOVA TABELA AG-GRID
        dbc.Row([
            dbc.Col([
                html.H4("Detalhamento de Títulos", style={'textAlign': 'center', 'marginBottom': '1rem'}),
                dag.AgGrid(
                    id='cr-ag-grid',
                    rowData=grid_rowData,
                    columnDefs=grid_columnDefs,
                    defaultColDef={
                        "sortable": True,
                        "filter": True,
                        "resizable": True,
                    },
                    columnSize="sizeToFit",
                    style={"height": "600px", "width": "100%"},
                    className="ag-theme-alpine", # Tema da tabela
                )
            ])
        ])
    ], className="content-container")
], fluid=True)


# --- 8. LAYOUT PRINCIPAL E ROTEADOR DASH ---

navbar = dbc.Navbar(
    dbc.Container(
        [
            html.A(
                dbc.Row(
                    [
                        dbc.Col(html.Img(src=app.get_asset_url('logo-confie.png'), height="40px")),
                        dbc.Col(dbc.NavbarBrand("Financeiro", className="ms-2")),
                    ],
                    align="center",
                    className="g-0",
                ),
                href="/financeiro/",
                style={"textDecoration": "none"},
            ),
        ]
    ),
    color="white",
    dark=False,
    fixed="top",
    className="custom-navbar"
)

app.layout = html.Div([
    dcc.Location(id='url-financeiro', refresh=False),
    navbar,
    html.Div(id='page-content-financeiro')
])

# Callback de Roteamento (Controla as páginas Dash)
@app.callback(
    Output('page-content-financeiro', 'children'),
    [Input('url-financeiro', 'pathname')]
)
def display_page(pathname):
    if pathname == '/financeiro/contas-a-receber':
        return layout_contas_a_receber
    elif pathname == '/financeiro/' or pathname == '/financeiro':
        return layout_home
    else:
        return html.Div([
            html.H3(f"Erro 404 - Página Dash não encontrada"),
            html.P(f"O caminho '{pathname}' não foi encontrado neste módulo."),
            dcc.Link("Voltar para Home", href="/financeiro/")
        ], style={'textAlign': 'center', 'padding': '50px'})


# --- 9. CALLBACK DE UPLOAD (ATUALIZADO) ---
# Agora atualiza o gráfico E a tabela

@app.callback(
    Output('graph-aging-list', 'figure'),
    Output('cr-upload-status', 'children'),
    Output('cr-ag-grid', 'rowData'),          # <-- NOVO OUTPUT
    Output('cr-ag-grid', 'columnDefs'),      # <-- NOVO OUTPUT
    Input('cr-upload-component', 'contents'),
    State('cr-upload-component', 'filename'),
    prevent_initial_call=True
)
def update_cr_from_upload(contents, filename):
    if contents is None:
        raise PreventUpdate

    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)

    try:
        if 'xls' not in filename:
            error_msg = html.Div("Erro: Por favor, envie um arquivo .xlsx ou .xls", style={'color': 'red'})
            return go.Figure(), error_msg, [], [] # Retorna dados vazios para a tabela

        # Lê o arquivo .xlsx ou .xls em um dataframe
        df_cr = pd.read_excel(io.BytesIO(decoded))

        # --- USA A NOVA FUNÇÃO DE PROCESSAMENTO ---
        fig_aging, rowData, columnDefs = process_cr_data(df_cr)
        # --- FIM ---

        message = f"Arquivo '{filename}' processado com sucesso ({len(rowData)} linhas)."
        return fig_aging, html.Div(message, style={'color': 'green'}), rowData, columnDefs

    except Exception as e:
        print(f"Erro no upload: {e}")
        error_msg = html.Div(f"Erro ao processar o arquivo: {e}", style={'color': 'red'})
        return go.Figure(), error_msg, [], [] # Retorna dados vazios


# --- 10. Bloco de Teste ---
if __name__ == '__main__':
    server.run(debug=True, port=8050)