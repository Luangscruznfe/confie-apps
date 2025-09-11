# dashboard_pbi/app.py --- VERSÃO FINAL COM CORREÇÃO DE TIPO DE DADO

import pandas as pd
import plotly.express as px
from flask import Flask, request, render_template, flash
import os

app = Flask(__name__)
app.secret_key = 'sua-chave-secreta-aqui-novamente'

# --- CARREGAMENTO DO CATÁLOGO DE PRODUTOS ---
CATALOGO_PATH = os.path.join(os.path.dirname(__file__), 'catalogo_produtos.xlsx')
try:
    colunas_posicoes = [0, 1, 5, 6]
    colunas_nomes_padrao = ['CODIGO', 'DESCRICAO', 'FABRICANTE', 'COD_BARRAS']

    catalogo_df = pd.read_excel(
        CATALOGO_PATH,
        usecols=colunas_posicoes,
        header=0
    )
    catalogo_df.columns = colunas_nomes_padrao
    print("SUCESSO: Arquivo 'catalogo_produtos.xlsx' carregado.")

except FileNotFoundError:
    catalogo_df = None
    print("AVISO: Arquivo 'catalogo_produtos.xlsx' não encontrado.")
except Exception as e:
    catalogo_df = None
    print(f"ERRO AO LER O CATÁLOGO XLSX: {e}")


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

                if 'ITENS' not in vendas_df.columns or 'VENDA' not in vendas_df.columns:
                    flash("ERRO DE ARQUIVO: O relatório enviado não contém as colunas obrigatórias 'ITENS' e 'VENDA'.")
                    return render_template('upload.html')
                
                if vendas_df.empty:
                    flash("ERRO DE CONTEÚDO: O arquivo não contém nenhuma linha de dados para analisar.")
                    return render_template('upload.html')
                
                # --- CORREÇÃO DO TIPO DE DADO ANTES DO MERGE ---
                # Garante que a chave 'ITENS' do relatório de vendas seja do tipo TEXTO
                vendas_df['ITENS'] = vendas_df['ITENS'].astype(str)

                if catalogo_df is not None:
                    # Garante que a chave 'DESCRICAO' do catálogo também seja do tipo TEXTO
                    catalogo_df['DESCRICAO'] = catalogo_df['DESCRICAO'].astype(str)
                    
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

                dados_completos_df['VENDA'] = pd.to_numeric(dados_completos_df['VENDA'], errors='coerce').fillna(0)

                top_10_itens = dados_completos_df.groupby('ITENS')['VENDA'].sum().nlargest(10).sort_values(ascending=True)
                fig_top_itens = px.bar(
                    top_10_itens, x='VENDA', y=top_10_itens.index,
                    orientation='h', title='Top 10 Itens Mais Vendidos', text_auto='.2s'
                )
                fig_top_itens.update_layout(yaxis_title="Item", xaxis_title="Total de Venda")

                if catalogo_df is not None and 'FABRICANTE' in dados_completos_df.columns:
                    vendas_por_fabricante = dados_completos_df.groupby('FABRICANTE')['VENDA'].sum().nlargest(15).sort_values(ascending=False)
                    fig_fabricantes = px.bar(
                        vendas_por_fabricante, x=vendas_por_fabricante.index, y='VENDA',
                        title='Top 15 Fabricantes por Venda', text_auto='.2s'
                    )
                    fig_fabricantes.update_layout(xaxis_title="Fabricante", yaxis_title="Total de Venda")
                    grafico_fabricantes_html = fig_fabricantes.to_html(full_html=False)
                else:
                    grafico_fabricantes_html = "<div class='alert alert-warning'>Gráfico de Fabricantes indisponível. Verifique se o arquivo 'catalogo_produtos.xlsx' foi enviado.</div>"

                return render_template(
                    'dashboard.html',
                    grafico1_html=fig_top_itens.to_html(full_html=False),
                    grafico2_html=grafico_fabricantes_html
                )

            except Exception as e:
                flash(f'Erro ao processar o arquivo: {e}')
                return render_template('upload.html')
        else:
            flash('Formato de arquivo inválido. Por favor, envie um arquivo .xlsx ou .xls')
            return render_template('upload.html')

    return render_template('upload.html')