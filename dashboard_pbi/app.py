# dashboard_pbi/app.py --- VERSÃO SIMPLIFICADA (SEM CATÁLOGO)

import pandas as pd
import plotly.express as px
from flask import Flask, request, render_template, flash
import os

app = Flask(__name__)
app.secret_key = 'sua_chave_secreta_aqui_final'

@app.route('/', methods=['GET', 'POST'])
def upload_analise():
    if request.method == 'POST':
        file = request.files.get('file')
        if not file or file.filename == '':
            flash('Nenhum arquivo selecionado.')
            return render_template('upload.html')

        try:
            # 1. Carrega a planilha de vendas (que já tem tudo)
            df = pd.read_excel(file, engine='openpyxl')
            df.columns = df.columns.str.strip().str.upper()

            # 2. Valida se as colunas essenciais existem
            colunas_necessarias = ['ITENS', 'VENDA', 'FABRICANTE']
            if not set(colunas_necessarias).issubset(df.columns):
                flash(f"ERRO: O relatório enviado não contém as colunas obrigatórias: {colunas_necessarias}.")
                return render_template('upload.html')
            
            if df.empty:
                flash("ERRO: O arquivo não contém nenhuma linha de dados.")
                return render_template('upload.html')

            # 3. Limpa os dados
            df['VENDA'] = pd.to_numeric(df['VENDA'], errors='coerce').fillna(0)
            # Remove linhas onde o fabricante ou o item esteja vazio
            df.dropna(subset=['ITENS', 'FABRICANTE'], inplace=True)
            df = df[df['VENDA'] > 0].copy()

            if df.empty:
                flash("Nenhum dado válido encontrado no relatório após a limpeza.")
                return render_template('upload.html')

            # 4. Gera os Gráficos
            # GRÁFICO 1: TOP 10 ITENS
            top_10_itens = df.groupby('ITENS')['VENDA'].sum().nlargest(10).sort_values(ascending=True)
            fig_top_itens = px.bar(
                top_10_itens, x='VENDA', y=top_10_itens.index,
                orientation='h', title='Top 10 Itens Mais Vendidos', text_auto='.2s'
            )
            fig_top_itens.update_layout(yaxis_title="Item", xaxis_title="Total de Venda")

            # GRÁFICO 2: TOP 15 FABRICANTES
            vendas_por_fabricante = df.groupby('FABRICANTE')['VENDA'].sum().nlargest(15).sort_values(ascending=False)
            fig_fabricantes = px.bar(
                vendas_por_fabricante, x=vendas_por_fabricante.index, y='VENDA',
                title='Top 15 Fabricantes por Venda', text_auto='.2s'
            )
            fig_fabricantes.update_layout(xaxis_tickangle=-45)
            
            # 5. Renderiza o Dashboard com os resultados
            return render_template(
                'dashboard.html',
                grafico_top_itens=fig_top_itens.to_html(full_html=False),
                grafico_fabricantes=fig_fabricantes.to_html(full_html=False)
            )

        except Exception as e:
            flash(f'Erro inesperado ao processar o arquivo: {e}')
            return render_template('upload.html')
            
    # Se o método for GET, apenas mostra a página de upload
    return render_template('upload.html')