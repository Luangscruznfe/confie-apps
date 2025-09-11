# dashboard_pbi/app.py --- VERSÃO DE DIAGNÓSTICO (COM INDENTAÇÃO 100% CORRIGIDA)

import pandas as pd
import plotly.express as px
from flask import Flask, request, render_template, flash
import os

app = Flask(__name__)
app.secret_key = 'sua-chave-secreta-aqui-novamente'

# --- PAINEL DE DIAGNÓSTICO (INICIALIZAÇÃO) ---
DEBUG_INFO = {
    "catalogo_status": "Não iniciado", "catalogo_error": "Nenhum",
    "catalogo_path_esperado": "N/D", "arquivos_no_diretorio": "N/D",
    "vendas_df_cols": "N/D", "catalogo_df_cols": "N/D", "merged_df_cols": "N/D"
}

# --- CARREGAMENTO DO CATÁLOGO (RODA UMA VEZ NA INICIALIZAÇÃO) ---
try:
    app_dir_path = os.path.dirname(__file__)
    CATALOGO_PATH = os.path.join(app_dir_path, 'catalogo_produtos.xlsx')
    DEBUG_INFO['catalogo_path_esperado'] = CATALOGO_PATH
    DEBUG_INFO['arquivos_no_diretorio'] = str(os.listdir(app_dir_path))

    colunas_posicoes = [0, 1, 5, 6]
    colunas_nomes_padrao = ['CODIGO', 'DESCRICAO', 'FABRICANTE', 'COD_BARRAS']
    catalogo_df = pd.read_excel(
        CATALOGO_PATH, usecols=colunas_posicoes, header=0
    )
    catalogo_df.columns = colunas_nomes_padrao
    
    DEBUG_INFO['catalogo_status'] = f"Carregado com Sucesso! Formato: {catalogo_df.shape}"
    DEBUG_INFO['catalogo_df_cols'] = str(catalogo_df.columns.tolist())
    print(f"SUCESSO: {DEBUG_INFO['catalogo_status']}")

except Exception as e:
    catalogo_df = None
    DEBUG_INFO['catalogo_status'] = "FALHA AO CARREGAR"
    DEBUG_INFO['catalogo_error'] = str(e)
    print(f"ERRO AO LER O CATÁLOGO: {e}")

@app.route('/', methods=['GET', 'POST'])
def pagina_upload():
    if request.method == 'POST':
        try:
            file = request.files.get('file')
            if not file or file.filename == '':
                flash('Nenhum arquivo selecionado')
                return render_template('upload.html', debug_info=DEBUG_INFO)

            if file and (file.filename.endswith('.xlsx') or file.filename.endswith('.xls')):
                vendas_df = pd.read_excel(file)
                DEBUG_INFO['vendas_df_cols'] = str(vendas_df.columns.tolist())

                if 'ITENS' not in vendas_df.columns or 'VENDA' not in vendas_df.columns:
                    flash("ERRO DE ARQUIVO: O relatório enviado não contém as colunas 'ITENS' e 'VENDA'.")
                    return render_template('upload.html', debug_info=DEBUG_INFO)
                
                if vendas_df.empty:
                    flash("ERRO DE CONTEÚDO: O arquivo não contém nenhuma linha de dados.")
                    return render_template('upload.html', debug_info=DEBUG_INFO)
                
                vendas_df['ITENS'] = vendas_df['ITENS'].astype(str)
                if catalogo_df is not None:
                    catalogo_df['DESCRICAO'] = catalogo_df['DESCRICAO'].astype(str)
                    dados_completos_df = pd.merge(
                        left=vendas_df, right=catalogo_df,
                        left_on='ITENS', right_on='DESCRICAO', how='left'
                    )
                    DEBUG_INFO['merged_df_cols'] = str(dados_completos_df.columns.tolist())
                else:
                    dados_completos_df = vendas_df
                    flash("Aviso: Catálogo de produtos não carregado.")

                dados_completos_df['VENDA'] = pd.to_numeric(dados_completos_df['VENDA'], errors='coerce').fillna(0)
                
                top_10_itens = dados_completos_df.groupby('ITENS')['VENDA'].sum().nlargest(10).sort_values(ascending=True)
                fig_top_itens = px.bar(
                    top_10_itens, x='VENDA', y=top_10_itens.index,
                    orientation='h', title='Top 10 Itens Mais Vendidos', text_auto='.2s'
                )
                fig_top_itens.update_layout(yaxis_title="Item", xaxis_title="Total de Venda")
                
                # Lógica do Gráfico de Fabricantes (Revisada para clareza)
                grafico_fabricantes_html = "<div class='alert alert-warning'>Gráfico de Fabricantes indisponível. Verifique se o arquivo 'catalogo_produtos.xlsx' foi enviado.</div>"
                if catalogo_df is not None and 'FABRICANTE' in dados_completos_df.columns:
                    df_fabricantes = dados_completos_df.dropna(subset=['FABRICANTE'])
                    if not df_fabricantes.empty:
                        vendas_por_fabricante = df_fabricantes.groupby('FABRICANTE')['VENDA'].sum().nlargest(15).sort_values(ascending=False)
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
                    grafico2_html=grafico_fabricantes_html,
                    debug_info=DEBUG_INFO
                )
        except Exception as e:
            flash(f'Erro ao processar o arquivo: {e}')
            if 'vendas_df' in locals():
                DEBUG_INFO['vendas_df_cols'] = str(vendas_df.columns.tolist())
            return render_template('upload.html', debug_info=DEBUG_INFO)
        else:
            flash('Formato de arquivo inválido.')
            return render_template('upload.html', debug_info=DEBUG_INFO)
            
    return render_template('upload.html', debug_info=DEBUG_INFO)