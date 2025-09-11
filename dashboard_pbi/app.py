# dashboard_pbi/app.py --- VERSÃO COM PAINEL DE DIAGNÓSTICO

import pandas as pd
import plotly.express as px
from flask import Flask, request, render_template, flash
import os

app = Flask(__name__)
app.secret_key = 'sua-chave-secreta-aqui-novamente'

# --- PAINEL DE DIAGNÓSTICO ---
# Estas variáveis vão guardar informações sobre o que acontece no servidor
DEBUG_INFO = {
    "catalogo_status": "Não iniciado",
    "catalogo_error": "Nenhum erro capturado.",
    "catalogo_path_esperado": "Não definido",
    "arquivos_no_diretorio": "Não verificado"
}

try:
    # Tenta obter informações do ambiente do servidor para o diagnóstico
    app_dir_path = os.path.dirname(__file__)
    CATALOGO_PATH = os.path.join(app_dir_path, 'catalogo_produtos.xlsx')
    DEBUG_INFO['catalogo_path_esperado'] = CATALOGO_PATH
    DEBUG_INFO['arquivos_no_diretorio'] = str(os.listdir(app_dir_path))

    # Tenta carregar o catálogo
    colunas_posicoes = [0, 1, 5, 6]
    colunas_nomes_padrao = ['CODIGO', 'DESCRICAO', 'FABRICANTE', 'COD_BARRAS']
    catalogo_df = pd.read_excel(
        CATALOGO_PATH, usecols=colunas_posicoes, header=0
    )
    catalogo_df.columns = colunas_nomes_padrao
    
    DEBUG_INFO['catalogo_status'] = f"Carregado com Sucesso! Formato da tabela: {catalogo_df.shape}"
    print(f"SUCESSO: {DEBUG_INFO['catalogo_status']}")

except Exception as e:
    # Se qualquer erro acontecer, ele será capturado aqui
    catalogo_df = None
    DEBUG_INFO['catalogo_status'] = "FALHA AO CARREGAR"
    DEBUG_INFO['catalogo_error'] = str(e) # Guarda a mensagem de erro exata
    print(f"ERRO AO LER O CATÁLOGO: {e}")

@app.route('/', methods=['GET', 'POST'])
def pagina_upload():
    if request.method == 'POST':
        # ... (a lógica de processamento continua a mesma de antes)
        if 'file' not in request.files:
            flash('Nenhum arquivo enviado')
            return render_template('upload.html', debug_info=DEBUG_INFO)
        file = request.files['file']
        if file.filename == '':
            flash('Nenhum arquivo selecionado')
            return render_template('upload.html', debug_info=DEBUG_INFO)
        if file and (file.filename.endswith('.xlsx') or file.filename.endswith('.xls')):
            try:
                vendas_df = pd.read_excel(file)
                if 'ITENS' not in vendas_df.columns or 'VENDA' not in vendas_df.columns:
                    flash("ERRO DE ARQUIVO: O relatório enviado não contém as colunas obrigatórias 'ITENS' e 'VENDA'.")
                    return render_template('upload.html', debug_info=DEBUG_INFO)
                if vendas_df.empty:
                    flash("ERRO DE CONTEÚDO: O arquivo não contém nenhuma linha de dados para analisar.")
                    return render_template('upload.html', debug_info=DEBUG_INFO)
                
                vendas_df['ITENS'] = vendas_df['ITENS'].astype(str)
                if catalogo_df is not None:
                    catalogo_df['DESCRICAO'] = catalogo_df['DESCRICAO'].astype(str)
                    dados_completos_df = pd.merge(
                        left=vendas_df, right=catalogo_df,
                        left_on='ITENS', right_on='DESCRICAO', how='left'
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
                    grafico2_html=grafico_fabricantes_html,
                    debug_info=DEBUG_INFO
                )
            except Exception as e:
                flash(f'Erro ao processar o arquivo: {e}')
                return render_template('upload.html', debug_info=DEBUG_INFO)
        else:
            flash('Formato de arquivo inválido.')
            return render_template('upload.html', debug_info=DEBUG_INFO)
            
    # Na primeira vez que carrega a página (GET)
    return render_template('upload.html', debug_info=DEBUG_INFO)