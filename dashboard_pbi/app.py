# dashboard_pbi/app.py --- VERSÃO DE PRODUÇÃO FINAL

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

                if 'CÓDIGO' not in vendas_df.columns or 'VENDA' not in vendas_df.columns or 'ITENS' not in vendas_df.columns:
                    flash("ERRO: O relatório enviado não contém as colunas obrigatórias 'CÓDIGO', 'ITENS' e 'VENDA'.")
                    return render_template('upload.html')
                
                if vendas_df.empty:
                    flash("ERRO: O arquivo não contém nenhuma linha de dados.")
                    return render_template('upload.html')
                
                dados_completos_df = vendas_df # Começa com a tabela de vendas
                if catalogo_df is not None:
                    # Garante que as chaves de cruzamento sejam do mesmo tipo (texto)
                    vendas_df['CÓDIGO'] = vendas_df['CÓDIGO'].astype(str)
                    catalogo_df['CODIGO'] = catalogo_df['CODIGO'].astype(str)
                    
                    dados_completos_df = pd.merge(
                        left=vendas_df, right=catalogo_df,
                        left_on='CÓDIGO', right_on='CODIGO',
                        how='left', suffixes=('_VENDA', '_CATALOGO') # Adiciona sufixos claros
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
                # Define a coluna de fabricante a ser usada
                coluna_fabricante = 'FABRICANTE_CATALOGO' if 'FABRICANTE_CATALOGO' in dados_completos_df.columns else 'FABRICANTE_VENDA'
                
                grafico_fabricantes_html = "<div class='alert alert-warning'>Coluna de Fabricante não encontrada.</div>"
                if coluna_fabricante in dados_completos_df.columns:
                    df_fabricantes = dados_completos_df.dropna(subset=[coluna_fabricante])
                    if not df_fabricantes.empty:
                        vendas_por_fabricante = df_fabricantes.groupby(coluna_fabricante)['VENDA'].sum().nlargest(15).sort_values(ascending=False)
                        fig_fabricantes = px.bar(
                            vendas_por_fabricante, x=vendas_por_fabricante.index, y='VENDA',
                            title='Top 15 Fabricantes por Venda', text_auto='.2s'
                        )
                        fig_fabricantes.update_layout(xaxis_title="Fabricante", yaxis_title="Total de Venda")
                        grafico_fabricantes_html = fig_fabricantes.to_html(full_html=False)
                    else:
                        grafico_fabricantes_html = "<div class='alert alert-info'>Nenhuma correspondência de fabricante encontrada.</div>"
                
                return render_template(
                    'dashboard.html',
                    grafico1_html=fig_top_itens.to_html(full_html=False),
                    grafico2_html=grafico_fabricantes_html
                )
        except Exception as e:
            flash(f'Erro inesperado ao processar o arquivo: {e}')
            return render_template('upload.html')
            
    return render_template('upload.html')