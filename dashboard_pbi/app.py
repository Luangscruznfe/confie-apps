# dashboard_pbi/app.py

import pandas as pd
import plotly.express as px
from flask import Flask, request, render_template, flash
import os

# Cria a instância da aplicação Flask para o dashboard
app = Flask(__name__)
app.secret_key = 'sua-chave-secreta-aqui-novamente' 

# --- CARREGAMENTO DO CATÁLOGO DE PRODUTOS ---
CATALOGO_PATH = os.path.join(os.path.dirname(__file__), 'catalogo_produtos.csv')
try:
    colunas_catalogo = ['CODIGO', 'DESCRICAO', 'FABRICANTE', 'DEPARTAMENTO']
    catalogo_df = pd.read_csv(CATALOGO_PATH, usecols=colunas_catalogo)
    print("SUCESSO: Arquivo 'catalogo_produtos.csv' carregado.")
except FileNotFoundError:
    catalogo_df = None
    print("AVISO: Arquivo 'catalogo_produtos.csv' não encontrado. Gráficos de fabricante e departamento serão desativados.")


@app.route('/', methods=['GET', 'POST'])
def pagina_upload():
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('Nenhum arquivo enviado')
            return render_template('upload.html')

        file = request.files['file']

        if file.filename == '':
            flash('Nenhum arquivo selecionado')
            return render_template('upload.html')

        if file and (file.filename.endswith('.xlsx') or file.filename.endswith('.xls')):
            try:
                vendas_df = pd.read_excel(file)

                # --- VALIDAÇÕES DO ARQUIVO ---
                if 'ITENS' not in vendas_df.columns or 'VENDA' not in vendas_df.columns:
                    flash("ERRO DE ARQUIVO: O relatório enviado não contém as colunas obrigatórias 'ITENS' e 'VENDA'. Por favor, verifique o arquivo e tente novamente.")
                    return render_template('upload.html')
                
                if vendas_df.empty:
                    flash("ERRO DE CONTEÚDO: O arquivo possui as colunas corretas, mas não contém nenhuma linha de dados para analisar.")
                    return render_template('upload.html')
                
                # --- LÓGICA DE MERGE ---
                if catalogo_df is not None:
                    dados_completos_df = pd.merge(
                        left=vendas_df, 
                        right=catalogo_df, 
                        left_on='ITENS', 
                        right_on='DESCRICAO', 
                        how='left'
                    )
                else:
                    dados_completos_df = vendas_df
                    flash("Aviso: Catálogo de produtos não carregado. Análise feita com dados limitados.")

                # --- LIMPEZA E PREPARAÇÃO DOS DADOS ---
                dados_completos_df['VENDA'] = pd.to_numeric(dados_completos_df['VENDA'], errors='coerce').fillna(0)


                # --- LÓGICA DOS NOVOS GRÁFICOS ---
                
                # GRÁFICO 1: TOP 10 ITENS MAIS VENDIDOS
                top_10_itens = dados_completos_df.groupby('ITENS')['VENDA'].sum().nlargest(10).sort_values(ascending=True)
                fig_top_itens = px.bar(
                    top_10_itens,
                    x='VENDA',
                    y=top_10_itens.index,
                    orientation='h',
                    title='Top 10 Itens Mais Vendidos',
                    text_auto='.2s'
                )
                fig_top_itens.update_layout(yaxis_title="Item", xaxis_title="Total de Venda")

                # GRÁFICO 2: VENDAS POR FABRICANTE (com segurança extra)
                # SÓ TENTA CRIAR O GRÁFICO SE O CATÁLOGO FOI CARREGADO
                if catalogo_df is not None and 'FABRICANTE' in dados_completos_df.columns:
                    vendas_por_fabricante = dados_completos_df.groupby('FABRICANTE')['VENDA'].sum().nlargest(15).sort_values(ascending=False)
                    fig_fabricantes = px.bar(
                        vendas_por_fabricante,
                        x=vendas_por_fabricante.index,
                        y='VENDA',
                        title='Top 15 Fabricantes por Venda',
                        text_auto='.2s'
                    )
                    fig_fabricantes.update_layout(xaxis_title="Fabricante", yaxis_title="Total de Venda")
                    grafico_fabricantes_html = fig_fabricantes.to_html(full_html=False)
                else:
                    grafico_fabricantes_html = "<div class='alert alert-warning'>Gráfico de Fabricantes indisponível. Verifique se o arquivo 'catalogo_produtos.csv' foi enviado corretamente.</div>"

                # GRÁFICO 3: VENDAS POR DEPARTAMENTO E ITEM (com segurança extra)
                # SÓ TENTA CRIAR O GRÁFICO SE O CATÁLOGO FOI CARREGADO
                if catalogo_df is not None and 'DEPARTAMENTO' in dados_completos_df.columns and 'ITENS' in dados_completos_df.columns:
                    df_treemap = dados_completos_df.dropna(subset=['DEPARTAMENTO'])
                    fig_treemap = px.treemap(
                        df_treemap,
                        path=[px.Constant("Todos Departamentos"), 'DEPARTAMENTO', 'ITENS'],
                        values='VENDA',
                        title='Vendas por Departamento e Itens (clique para explorar)'
                    )
                    grafico_treemap_html = fig_treemap.to_html(full_html=False)
                else:
                    grafico_treemap_html = "<div class='alert alert-warning'>Gráfico de Departamentos indisponível. Verifique se o arquivo 'catalogo_produtos.csv' foi enviado corretamente.</div>"


                return render_template(
                    'dashboard.html', 
                    grafico1_html=fig_top_itens.to_html(full_html=False), 
                    grafico2_html=grafico_fabricantes_html,
                    grafico3_html=grafico_treemap_html
                )

            except Exception as e:
                flash(f'Erro ao processar o arquivo: {e}')
                return render_template('upload.html')
        else:
            flash('Formato de arquivo inválido. Por favor, envie um arquivo .xlsx ou .xls')
            return render_template('upload.html')

    return render_template('upload.html')