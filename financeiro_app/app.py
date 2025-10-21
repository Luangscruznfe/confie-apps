# =====================================================================
# CÓDIGO FINAL CORRIGIDO PARA /financeiro_app/app.py
# =====================================================================

import os
import pandas as pd
from flask import Flask, render_template, request, flash, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY')
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

class PerfilCliente(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    codigo_cliente = db.Column(db.String, unique=True, nullable=False)
    nome_cliente = db.Column(db.String, nullable=False)
    dias_atraso_medio = db.Column(db.Float, default=0)
    pontualidade = db.Column(db.Float, default=0)
    titulos_atrasados = db.Column(db.Integer, default=0)
    total_titulos = db.Column(db.Integer, default=0)
    valor_total_pago = db.Column(db.Float, default=0)


@app.cli.command("init-db")
def init_db_command():
    """Cria as tabelas do banco de dados."""
    db.create_all()
    print("Banco de dados inicializado.")


@app.route("/", methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
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
                    df['VL. PAGO'] = (
                        df['VL. PAGO']
                        .str.replace('.', '', regex=False)
                        .str.replace(',', '.', regex=False)
                        .astype(float)
                    )

                df['DIAS_ATRASO'] = (df['DT. PAGTO'] - df['VENCTO']).dt.days

                perfil_clientes_df = df.groupby(['CÓDIGO', 'NOME']).agg(
                    DIAS_ATRASO_MEDIO=('DIAS_ATRASO', lambda x: x[x > 0].mean()),
                    TITULOS_ATRASADOS=('DIAS_ATRASO', lambda x: (x > 0).sum()),
                    TITULOS_EM_DIA=('DIAS_ATRASO', lambda x: (x <= 0).sum()),
                    VALOR_TOTAL_PAGO=('VL. PAGO', 'sum')
                ).reset_index()

                perfil_clientes_df['TOTAL_TITULOS'] = (
                    perfil_clientes_df['TITULOS_ATRASADOS'] + perfil_clientes_df['TITULOS_EM_DIA']
                )

                perfil_clientes_df['PONTUALIDADE_%'] = (
                    (perfil_clientes_df['TITULOS_EM_DIA'] / perfil_clientes_df['TOTAL_TITULOS']) * 100
                )

                perfil_clientes_df['DIAS_ATRASO_MEDIO'] = perfil_clientes_df['DIAS_ATRASO_MEDIO'].fillna(0)

                # ✅ Corrige o nome da coluna aqui
                perfil_clientes_df.rename(columns={"PONTUALIDADE_%": "PONTUALIDADE"}, inplace=True)

                # Apaga os dados antigos para inserir os novos
                db.session.query(PerfilCliente).delete()

                for _, row in perfil_clientes_df.iterrows():
                    novo_perfil = PerfilCliente(
                        codigo_cliente=str(row['CÓDIGO']),
                        nome_cliente=row['NOME'],
                        dias_atraso_medio=row['DIAS_ATRASO_MEDIO'],
                        pontualidade=row['PONTUALIDADE'],
                        titulos_atrasados=row['TITULOS_ATRASADOS'],
                        total_titulos=row['TOTAL_TITULOS'],
                        valor_total_pago=row['VALOR_TOTAL_PAGO']
                    )
                    db.session.add(novo_perfil)
                
                db.session.commit()
                flash('Base de dados atualizada com sucesso!', 'success')

            except Exception as e:
                db.session.rollback()
                flash(f'Ocorreu um erro ao processar o arquivo: {e}', 'error')
            
            return redirect(url_for('index'))

    # ----- GET (mostrar a página) -----
    clientes_db = PerfilCliente.query.order_by(PerfilCliente.dias_atraso_medio.desc()).all()

    dados_clientes = [
        {
            'CÓDIGO': c.codigo_cliente,
            'NOME': c.nome_cliente,
            'DIAS_ATRASO_MEDIO': c.dias_atraso_medio,
            'PONTUALIDADE': c.pontualidade,
            'TITULOS_ATRASADOS': c.titulos_atrasados,
            'TOTAL_TITULOS': c.total_titulos,
            'VALOR_TOTAL_PAGO': c.valor_total_pago
        }
        for c in clientes_db
    ]

    kpis = {"total_clientes": 0, "pontualidade_media": 0, "valor_total": 0, "media_geral_atraso": 0}

    if dados_clientes:
        total_clientes = len(dados_clientes)
        pontualidade_media = sum(c['PONTUALIDADE'] for c in dados_clientes) / total_clientes
        valor_total = sum(c['VALOR_TOTAL_PAGO'] for c in dados_clientes)
        soma_atrasos_ponderada = sum(c['DIAS_ATRASO_MEDIO'] * c['TITULOS_ATRASADOS'] for c in dados_clientes)
        total_titulos_atrasados = sum(c['TITULOS_ATRASADOS'] for c in dados_clientes)
        media_geral_atraso = soma_atrasos_ponderada / total_titulos_atrasados if total_titulos_atrasados > 0 else 0

        kpis = {
            "total_clientes": total_clientes,
            "pontualidade_media": pontualidade_media,
            "valor_total": valor_total,
            "media_geral_atraso": media_geral_atraso
        }

    return render_template('dashboard_financeiro.html', clientes=dados_clientes, kpis=kpis)
