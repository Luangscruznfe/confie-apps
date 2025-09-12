# dashboard_pbi/app.py --- VERSÃO FINAL E SIMPLES (SEM CATÁLOGO)

import pandas as pd
import plotly.express as px
from flask import Flask, request, render_template, flash
import os

app = Flask(__name__)
app.secret_key = 'chave_final_simples'

@app.route('/', methods=['GET', 'POST'])
def dashboard():
    # Se a página é carregada pela primeira vez (GET), não há resultados.
    if request.method == 'GET':
        return render_template('dashboard.html', resultados=None)

    # Se um arquivo é enviado (POST)
    if request.method == 'POST':
        file = request.files.get('file')
        if not file or file.filename == '':
            flash('Nenhum arquivo selecionado.')
            return render_template('dashboard.html', resultados=None)

        try:
            # 1. Carrega a planilha de vendas (que já tem tudo)
            df = pd.read_excel(file, engine='openpyxl')
            df.columns = df.columns.str.strip().str.upper()

            # 2. Valida se as colunas essenciais existem
            colunas_necessarias = ['ITENS', 'VENDA', 'FABRICANTE']
            if not set(colunas_necessarias).issubset(df.columns):
                flash(f"ERRO: O relatório não contém as colunas obrigatórias: {colunas_necessarias}.")
                return render_template('dashboard.html', resultados=None)
            
            if df.empty:
                flash("ERRO: O arquivo não contém nenhuma linha de dados.")
                return render_template('dashboard.html', resultados=None)

            # 3. Limpa os dados
            df['VENDA'] = pd.to_numeric(df['VENDA'], errors='coerce').fillna(0)
            df.dropna(subset=['ITENS', 'FABRICANTE'], inplace=True)
            df = df[df['VENDA'] > 0].copy()

            if df.empty:
                flash("Nenhum dado válido encontrado no relatório após a limpeza.")
                return render_template('dashboard.html', resultados=None)

            # 4. Gera os Gráficos
            top_10_itens = df.groupby('ITENS')['VENDA'].sum().nlargest(10).sort_values(ascending=True)
            fig_top_itens = px.bar(
                top_10_itens, x='VENDA', y=top_10_itens.index,
                orientation='h', title='Top 10 Itens Mais Vendidos', text_auto='.2s'
            )
            
            vendas_por_fabricante = df.groupby('FABRICANTE')['VENDA'].sum().nlargest(15).sort_values(ascending=False)
            fig_fabricantes = px.bar(
                vendas_por_fabricante, x=vendas_por_fabricante.index, y='VENDA',
                title='Top 15 Fabricantes por Venda', text_auto='.2s'
            )
            fig_fabricantes.update_layout(xaxis_tickangle=-45)
            
            resultados = {
                "grafico_top_itens": fig_top_itens.to_html(full_html=False),
                "grafico_fabricantes": fig_fabricantes.to_html(full_html=False)
            }
            
            return render_template('dashboard.html', resultados=resultados)

        except Exception as e:
            flash(f'Erro inesperado ao processar o arquivo: {e}')
            return render_template('dashboard.html', resultados=None)