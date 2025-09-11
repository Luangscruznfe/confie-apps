# dashboard_pbi/app.py --- VERSÃO FINAL COM CRUZAMENTO PELA DESCRIÇÃO

import pandas as pd
import plotly.express as px
from flask import Flask, request, render_template, flash
import os

app = Flask(__name__)
app.secret_key = 'sua-chave-secreta-aqui-novamente'

# --- CARREGAMENTO DO CATÁLOGO DE PRODUTOS ---
catalogo_df = None
try:
    CATALOGO_PATH = os.path.join(os.path.dirname(__file__), 'catalogo_produtos.xlsx')
    colunas_posicoes = [0, 1, 5, 6]
    colunas_nomes_padrao = ['CODIGO', 'DESCRICAO', 'FABRICANTE', 'COD_BARRAS']
    catalogo_df = pd.read_excel(
        CATALOGO_PATH, usecols=colunas_posicoes, header=0
    )
    catalogo_df.columns = colunas_nomes_padrao
    print("SUCESSO: Arquivo 'catalogo_produtos.xlsx' carregado.")
except Exception as e:
    print(f"AVISO AO LER O CATÁLOGO: {e}")

@app.route('/', methods=['GET', 'POST'])
def pagina_upload():
    if request.method == 'POST':
        try:
            file = request.files.get('file')
            if not file or file.filename == '':
                flash('Nenhum arquivo selecionado')
                return render_template('upload.html')

            if file and (file.filename.endswith('.xlsx') or file.filename.endswith('.xls')):
                vendas_df = pd.read_excel(file)

                if 'ITENS' not in vendas_df.columns or 'VENDA' not in vendas_df.columns:
                    flash("ERRO: O relatório enviado não contém as colunas obrigatórias 'ITENS' e 'VENDA'.")
                    return render_template('upload.html')
                
                if vendas_df.empty:
                    flash("ERRO: O arquivo não contém nenhuma linha de dados.")
                    return render_template('upload.html')
                
                dados_completos_df = vendas_df
                if catalogo_df is not None:
                    # Garante que as colunas de DESCRIÇÃO em ambas as tabelas sejam do mesmo tipo (texto)
                    vendas_df['ITENS'] = vendas_df['ITENS'].astype(str)
                    catalogo_df['DESCRICAO'] = catalogo_df['DESCRICAO'].astype(str)
                    
                    dados_completos_df = pd.merge(
                        left=vendas_df, right=catalogo_df,
                        left_on='ITENS',      # <-- USA A DESCRIÇÃO DO ITEM DO RELATÓRIO
                        right_on='DESCRICAO', # <-- USA A DESCRIÇÃO DO ITEM DO CATÁLOGO
                        how='left', suffixes=('_VENDA', '_CATALOGO')
                    )

                dados_completos_df['VENDA'] = pd.to_numeric(dados_completos_df['VENDA'], errors='coerce').fillna(0)
                
                # GRÁFICO 1: TOP 10 ITENS
                top_10_itens = dados_completos_df.groupby('ITENS')['VENDA'].sum().nlargest(10).sort_values(ascending=True)
                fig_top_itens = px.bar(
                    top_10_itens, x='VENDA', y=top_10_itens.index,
                    orientation='h', title='Top 10 Itens Mais Vendidos', text_auto='.2s'
                )
                fig_top_itens.update_layout(yaxis_title="Item", xaxis_title="Total de Venda")
                
                # GRÁFICO 2: TOP 15 FABRICANTES
                grafico_fabricantes_html = "<div class='alert alert-warning'>Gráfico de Fabricantes indisponível.</div>"
                coluna_fabricante_final = None

                if 'FABRICANTE_CATALOGO' in dados_completos_df.columns:
                    coluna_fabricante_final = 'FABRICANTE_CATALOGO'
                elif 'FABRICANTE' in dados_completos_df.columns:
                    coluna_fabricante_final = 'FABRICANTE'

                if coluna_fabricante_final:
                    df_fabricantes = dados_completos_df.dropna(subset=[coluna_fabricante_final])
                    if not df_fabricantes.empty:
                        vendas_por_fabricante = df_fabricantes.groupby(coluna_fabricante_final)['VENDA'].sum().nlargest(15).sort_values(ascending=False)
                        fig_fabricantes = px.bar(
                            vendas_por_fabricante, x=vendas_por_fabricante.index, y='VENDA',
                            title='Top 15 Fabricantes por Venda', text_auto='.2s'
                        )
                        fig_fabricantes.update_layout(xaxis_title="Fabricante", yaxis_title="Total de Venda")
                        grafico_fabricantes_html = fig_fabricantes.to_html(full_html=False)
                    else:
                        grafico_fabricantes_html = "<div class='alert alert-info'>Nenhuma correspondência de fabricante encontrada entre o relatório e o catálogo.</div>"
                
                return render_template(
                    'dashboard.html',
                    grafico1_html=fig_top_itens.to_html(full_html=False),
                    grafico2_html=grafico_fabricantes_html
                )
        except Exception as e:
            flash(f'Erro inesperado ao processar o arquivo: {e}')
            return render_template('upload.html')
            
    return render_template('upload.html')