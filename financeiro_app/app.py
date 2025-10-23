# =====================================================================
# CÓDIGO ATUALIZADO PARA /financeiro_app/app.py (Tendências por Data de Pagamento)
# =====================================================================

import os
import pandas as pd
from flask import Flask, render_template, request, flash, redirect, url_for, jsonify
from flask_sqlalchemy import SQLAlchemy
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import func, cast, Date # Importa cast e Date

load_dotenv()

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY')
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# --- Modelos do Banco de Dados ---
class UploadBatch(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    data_upload = db.Column(db.DateTime, default=datetime.utcnow)
    nome_arquivo = db.Column(db.String(200))
    perfis = db.relationship('PerfilCliente', back_populates='batch', lazy='dynamic', cascade="all, delete-orphan")

class PerfilCliente(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    codigo_cliente = db.Column(db.String, nullable=False)
    nome_cliente = db.Column(db.String, nullable=False)
    # Estes campos agora representam o perfil GERAL do cliente no momento do upload
    dias_atraso_medio = db.Column(db.Float, default=0)
    pontualidade = db.Column(db.Float, default=0)
    titulos_atrasados = db.Column(db.Integer, default=0) # Total de títulos atrasados ATÉ aquele upload
    total_titulos = db.Column(db.Integer, default=0)     # Total de títulos ATÉ aquele upload
    valor_total_pago = db.Column(db.Float, default=0)    # Valor total pago ATÉ aquele upload
    risco = db.Column(db.String(10), default='Médio')
    # Este campo representa a data específica de pagamento DESTE título
    data_pagto = db.Column(db.Date)
    # Valor específico deste título
    valor_titulo_pago = db.Column(db.Float, default=0) # NOVA COLUNA: Valor do título individual
    # Dias de atraso específico deste título
    dias_atraso_titulo = db.Column(db.Integer, default=0) # NOVA COLUNA: Atraso do título individual

    batch_id = db.Column(db.Integer, db.ForeignKey('upload_batch.id'))
    batch = db.relationship('UploadBatch', back_populates='perfis')

@app.cli.command("init-db")
def init_db_command():
    """Cria/Atualiza as tabelas do banco de dados."""
    # Adicionado para garantir que as tabelas sejam atualizadas
    with app.app_context():
        db.create_all()
    print("Banco de dados inicializado/atualizado.")


@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == 'POST':
        # ... (verificação de arquivo) ...
        if 'file' not in request.files or request.files['file'].filename == '':
            flash('Nenhum arquivo selecionado', 'error')
            return redirect(url_for('index'))
        file = request.files['file']

        if file and file.filename.endswith('.xlsx'):
            try:
                df = pd.read_excel(file)
                df.columns = df.columns.str.strip()

                df['VENCTO'] = pd.to_datetime(df['VENCTO'], dayfirst=True, errors='coerce')
                df['DT. PAGTO'] = pd.to_datetime(df['DT. PAGTO'], dayfirst=True, errors='coerce')
                df.dropna(subset=['VENCTO', 'DT. PAGTO'], inplace=True)

                if 'VL. PAGO' in df.columns and df['VL. PAGO'].dtype == 'object':
                     df['VL. PAGO'] = df['VL. PAGO'].str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)
                # MUDANÇA: Calcular atraso ANTES de agrupar
                df['DIAS_ATRASO_TITULO'] = (df['DT. PAGTO'] - df['VENCTO']).dt.days

                # Calculos de perfil (agrupando por cliente para o Risco e Pontualidade GERAL)
                perfil_clientes_df = df.groupby(['CÓDIGO', 'NOME']).agg(
                    DIAS_ATRASO_MEDIO_GERAL=('DIAS_ATRASO_TITULO', lambda x: x[x > 0].mean()), # Média geral do cliente
                    TITULOS_ATRASADOS_GERAL=('DIAS_ATRASO_TITULO', lambda x: (x > 0).sum()), # Total atrasado do cliente
                    TITULOS_EM_DIA_GERAL=('DIAS_ATRASO_TITULO', lambda x: (x <= 0).sum()), # Total em dia do cliente
                    VALOR_TOTAL_PAGO_GERAL=('VL. PAGO', 'sum') # Valor total do cliente
                ).reset_index()

                perfil_clientes_df['TOTAL_TITULOS_GERAL'] = perfil_clientes_df['TITULOS_ATRASADOS_GERAL'] + perfil_clientes_df['TITULOS_EM_DIA_GERAL']
                perfil_clientes_df['PONTUALIDADE_GERAL'] = (perfil_clientes_df['TITULOS_EM_DIA_GERAL'] / perfil_clientes_df['TOTAL_TITULOS_GERAL']) * 100
                perfil_clientes_df['DIAS_ATRASO_MEDIO_GERAL'] = perfil_clientes_df['DIAS_ATRASO_MEDIO_GERAL'].fillna(0)

                def calcular_risco(row):
                    atraso = row['DIAS_ATRASO_MEDIO_GERAL']
                    pontualidade = row['PONTUALIDADE_GERAL']
                    if atraso <= 5 and pontualidade > 80: return 'Baixo'
                    elif atraso > 15 or pontualidade < 50: return 'Alto'
                    else: return 'Médio'
                perfil_clientes_df['RISCO_GERAL'] = perfil_clientes_df.apply(calcular_risco, axis=1)

                # --- Lógica de Salvamento ---
                novo_batch = UploadBatch(nome_arquivo=file.filename)
                db.session.add(novo_batch)
                db.session.flush()

                # Mapear os dados agregados para consulta rápida
                perfis_dict = perfil_clientes_df.set_index('CÓDIGO').to_dict('index')

                # Salvar CADA TÍTULO individualmente
                for index, row in df.iterrows():
                    codigo_cliente_str = str(row['CÓDIGO'])
                    # Tenta converter para float para buscar no dicionário, caso o código seja numérico no Excel
                    try:
                       codigo_lookup = float(codigo_cliente_str)
                    except ValueError:
                       codigo_lookup = codigo_cliente_str # Mantém como string se não for número

                    perfil_agregado = perfis_dict.get(codigo_lookup)

                    if perfil_agregado:
                        novo_perfil = PerfilCliente(
                            codigo_cliente=codigo_cliente_str,
                            nome_cliente=row['NOME'],
                            dias_atraso_medio=perfil_agregado['DIAS_ATRASO_MEDIO_GERAL'],
                            pontualidade=perfil_agregado['PONTUALIDADE_GERAL'],
                            titulos_atrasados=perfil_agregado['TITULOS_ATRASADOS_GERAL'],
                            total_titulos=perfil_agregado['TOTAL_TITULOS_GERAL'],
                            valor_total_pago=perfil_agregado['VALOR_TOTAL_PAGO_GERAL'], # Salva o valor total GERAL do cliente
                            risco=perfil_agregado['RISCO_GERAL'],
                            data_pagto=row['DT. PAGTO'].date(),
                            valor_titulo_pago=row['VL. PAGO'],       # Salva o valor DESTE título
                            dias_atraso_titulo=row['DIAS_ATRASO_TITULO'], # Salva o atraso DESTE título
                            batch=novo_batch
                        )
                        db.session.add(novo_perfil)

                db.session.commit()
                flash('Base de dados atualizada com sucesso!', 'success')

            except Exception as e:
                db.session.rollback()
                print(f"!!!!!!!!!!!!!! ERRO DETALHADO NO UPLOAD !!!!!!!!!!!!!!\n{e}\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                flash(f'Ocorreu um erro ao processar o arquivo: {e}', 'error')

            return redirect(url_for('index'))

    # ----- GET (mostrar a página) -----
    latest_batch = UploadBatch.query.order_by(UploadBatch.data_upload.desc()).first()
    dados_clientes = []
    if latest_batch:
        # Busca os registros do último lote
        clientes_do_lote = PerfilCliente.query.filter_by(batch_id=latest_batch.id).all()
        # Agrupa no Python para exibir uma linha por cliente no dashboard
        clientes_agrupados = {}
        for c in clientes_do_lote:
            if c.codigo_cliente not in clientes_agrupados:
                # Usa os valores GERAIS que foram salvos para cada registro do cliente
                clientes_agrupados[c.codigo_cliente] = {
                    'CÓDIGO': c.codigo_cliente,
                    'NOME': c.nome_cliente,
                    'DIAS_ATRASO_MEDIO': c.dias_atraso_medio,
                    'PONTUALIDADE': c.pontualidade,
                    'RISCO': c.risco,
                    'TITULOS_ATRASADOS': c.titulos_atrasados, # Total de títulos atrasados do cliente
                    'TOTAL_TITULOS': c.total_titulos, # Total de títulos do cliente
                    'VALOR_TOTAL_PAGO': 0.0 # Será somado apenas para o KPI, a tabela mostra o valor GERAL
                }
            # Soma o valor individual de cada título para o KPI 'Valor Total Analisado'
            clientes_agrupados[c.codigo_cliente]['VALOR_TOTAL_PAGO'] += c.valor_titulo_pago

        # Pega o VALOR_TOTAL_PAGO_GERAL do último registro encontrado para cada cliente para exibir na tabela
        last_records = {}
        for c in clientes_do_lote:
             last_records[c.codigo_cliente] = c.valor_total_pago # Pega o valor total GERAL

        for codigo, cliente_data in clientes_agrupados.items():
             cliente_data['VALOR_TOTAL_PAGO_DISPLAY'] = last_records.get(codigo, 0.0) # Valor para mostrar na tabela

        dados_clientes = sorted(list(clientes_agrupados.values()), key=lambda x: x['DIAS_ATRASO_MEDIO'], reverse=True)

    # --- Cálculo de KPIs (baseado nos dados_clientes agrupados) ---
    kpis = {"total_clientes": 0, "pontualidade_media": 0, "valor_total": 0, "media_geral_atraso": 0, "perc_receita_em_risco": 0}
    if dados_clientes:
        total_clientes = len(dados_clientes)
        pontualidade_media = sum(c['PONTUALIDADE'] for c in dados_clientes) / total_clientes if total_clientes > 0 else 0
        valor_total = sum(c['VALOR_TOTAL_PAGO'] for c in dados_clientes) # Soma dos valores individuais dos títulos do último batch

        # Média geral de atraso e receita em risco baseada nos perfis GERAIS dos clientes
        soma_atrasos_ponderada = sum(c['DIAS_ATRASO_MEDIO'] * c['TITULOS_ATRASADOS'] for c in dados_clientes if c['TOTAL_TITULOS'] > 0)
        total_titulos_atrasados_geral = sum(c['TITULOS_ATRASADOS'] for c in dados_clientes)
        media_geral_atraso = soma_atrasos_ponderada / total_titulos_atrasados_geral if total_titulos_atrasados_geral > 0 else 0
        valor_em_risco = sum(c['VALOR_TOTAL_PAGO_DISPLAY'] for c in dados_clientes if c['RISCO'] == 'Alto') # Usa o valor GERAL do cliente
        perc_receita_em_risco = (valor_em_risco / valor_total) * 100 if valor_total > 0 else 0

        kpis = {
            "total_clientes": total_clientes,
            "pontualidade_media": pontualidade_media,
            "valor_total": valor_total, # Soma dos títulos do último batch
            "media_geral_atraso": media_geral_atraso,
            "perc_receita_em_risco": perc_receita_em_risco
        }

    return render_template('dashboard_financeiro.html', clientes=dados_clientes, kpis=kpis)

# --- Rota de Tendências (agora por mês de pagamento) ---
@app.route("/tendencias")
def tendencias():
    # MUDANÇA PRINCIPAL: Agrupar por mês da data de pagamento
    dados_evolucao = db.session.query(
        func.date_trunc('month', PerfilCliente.data_pagto).label('mes_pagto'),
        # Média da Pontualidade GERAL dos clientes que pagaram naquele mês
        func.avg(PerfilCliente.pontualidade).label('pontualidade_media'),
        # Soma dos Títulos Atrasados GERAIS dos clientes que pagaram naquele mês (pode ser repetitivo, melhor usar o atraso do título)
        # func.sum(PerfilCliente.titulos_atrasados).label('total_titulos_atrasados_geral'),
        # Média dos dias de atraso dos TÍTULOS pagos naquele mês
        func.avg(PerfilCliente.dias_atraso_titulo).label('atraso_medio_titulos'),
        # Soma dos valores dos TÍTULOS pagos naquele mês
        func.sum(PerfilCliente.valor_titulo_pago).label('valor_total_mes'),
        # Soma dos valores dos TÍTULOS de clientes de ALTO RISCO pagos naquele mês
        func.sum(
            case((PerfilCliente.risco == 'Alto', PerfilCliente.valor_titulo_pago), else_=0)
        ).label('valor_risco_mes'),
        # Contagem de TÍTULOS atrasados pagos naquele mês
         func.sum(
            case((PerfilCliente.dias_atraso_titulo > 0, 1), else_=0)
        ).label('contagem_titulos_atrasados_mes')

    ).group_by('mes_pagto').order_by('mes_pagto').all()

    labels = [d.mes_pagto.strftime('%Y-%m') for d in dados_evolucao]
    valores_pontualidade = [round(float(d.pontualidade_media or 0), 2) for d in dados_evolucao]
    valores_atraso_medio_titulos = [round(float(d.atraso_medio_titulos or 0), 1) for d in dados_evolucao]
    valores_contagem_atrasados = [int(d.contagem_titulos_atrasados_mes or 0) for d in dados_evolucao]
    valores_receita = [round(float(d.valor_total_mes or 0), 2) for d in dados_evolucao]
    valores_receita_risco = [round(float(d.valor_risco_mes or 0), 2) for d in dados_evolucao]

    # Distribuição de risco (do último mês com pagamentos)
    ultimo_mes_pagto = db.session.query(func.max(PerfilCliente.data_pagto)).scalar()
    dados_risco = {'Baixo': 0, 'Médio': 0, 'Alto': 0}
    if ultimo_mes_pagto:
        # Pega clientes distintos que pagaram no último mês
        clientes_ultimo_mes = db.session.query(
                PerfilCliente.codigo_cliente, PerfilCliente.risco
            ).filter(func.date_trunc('month', PerfilCliente.data_pagto) == func.date_trunc('month', ultimo_mes_pagto))\
             .distinct(PerfilCliente.codigo_cliente)\
             .all()
        # Conta a distribuição de risco desses clientes
        for _, risco_cliente in clientes_ultimo_mes:
            if risco_cliente in dados_risco:
                dados_risco[risco_cliente] += 1

    labels_risco = list(dados_risco.keys())
    valores_risco = list(dados_risco.values())

    return render_template("tendencias.html",
                           labels=labels, # Labels unificados por mês
                           valores_pontualidade=valores_pontualidade,
                           valores_atraso_medio_titulos=valores_atraso_medio_titulos, # Nova métrica
                           valores_contagem_atrasados=valores_contagem_atrasados, # Nova métrica
                           labels_risco=labels_risco,
                           valores_risco=valores_risco,
                           valores_receita=valores_receita,
                           valores_receita_risco=valores_receita_risco)

# --- Rota para Limpar Base ---
@app.route("/limpar_base", methods=["POST"])
def limpar_base():
    try:
        # Apaga todos os clientes E batches usando cascade delete
        num_batches_deleted = UploadBatch.query.delete()
        db.session.commit()
        flash(f"Base de dados limpa com sucesso! {num_batches_deleted} lotes de upload foram removidos.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Erro ao limpar base: {str(e)}", "danger")
    return redirect(url_for("index"))

# Adiciona a função 'case' ao ambiente Jinja (necessário para a query de tendências)
from sqlalchemy import case
app.jinja_env.globals.update(case=case)