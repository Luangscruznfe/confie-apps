# dashboard_pbi/app.py

import pandas as pd
import plotly.express as px
from flask import Flask, request, render_template, flash
import os

# Cria a instância da aplicação Flask para o dashboard
app = Flask(__name__)
app.secret_key = 'sua-chave-secreta-aqui-novamente' 

# --- CARREGAMENTO DO CATÁLOGO DE PRODUTOS ---
# Otimização: Carrega o catálogo de produtos apenas uma vez quando a aplicação inicia.
CATALOGO_PATH = os.path.join(os.path.dirname(__file__), 'catalogo_produtos.csv')
try:
    # Especifica as colunas a serem usadas para economizar memória
    colunas_catalogo = ['CODIGO', 'DESCRICAO', 'FABRICANTE', 'DEPARTAMENTO']
    catalogo_df = pd.read_csv(CATALOGO_PATH, usecols=colunas_catalogo)
except FileNotFoundError:
    # Se o arquivo de catálogo não for encontrado, a aplicação ainda roda, mas com um aviso.
    catalogo_df = None
    print("AVISO: Arquivo 'catalogo_produtos.csv' não encontrado. As informações de fabricante e departamento não estarão disponíveis.")


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
                # 1. Carrega o relatório de vendas enviado pelo usuário
                vendas_df = pd.read_excel(file)

                # --- ENRIQUECIMENTO DOS DADOS ---
                if catalogo_df is not None:
                    # 2. Junta o relatório de vendas com o catálogo
                    # A conexão é feita entre a coluna 'ITENS' (das vendas) e a 'DESCRICAO' (do catálogo)
                    dados_completos_df = pd.merge(
                        left=vendas_df, 
                        right=catalogo_df, 
                        left_on='ITENS', 
                        right_on='DESCRICAO', 
                        how='left' # 'left' mantém todos os itens do relatório de vendas, mesmo que não encontre no catálogo
                    )
                else:
                    # Se o catálogo não foi carregado, usa os dados originais
                    dados_completos_df = vendas_df
                    flash("Aviso: Catálogo de produtos não carregado. Análise feita com dados limitados.")

                # --- LIMPEZA E PREPARAÇÃO DOS DADOS ---
                # Garante que a coluna 'VENDA' é um número para poder fazer cálculos
                # Remove o 'R$', espaços e converte a vírgula do decimal para ponto.
                if 'VENDA' in dados_completos_df.columns:
                    dados_completos_df['VENDA'] = dados_completos_df['VENDA'].astype(str).str.replace('R$', '', regex=False).str.strip().str.replace(',', '.', regex=False).astype(float)
                else:
                    raise ValueError("A coluna 'VENDA' não foi encontrada no relatório.")


                # --- LÓGICA DOS NOVOS GRÁFICOS ---

                # GRÁFICO 1: TOP 10 ITENS MAIS VENDIDOS (Gráfico de Barras Horizontal)
                top_10_itens = dados_completos_df.groupby('ITENS')['VENDA'].sum().nlargest(10).sort_values(ascending=True)
                fig_top_itens = px.bar(
                    top_10_itens,
                    x='VENDA',
                    y=top_10_itens.index,
                    orientation='h',
                    title='Top 10 Itens Mais Vendidos',
                    text_auto='.2s' # Formato do texto (ex: 1.2k)
                )
                fig_top_itens.update_layout(yaxis_title="Item", xaxis_title="Total de Venda")

                # GRÁFICO 2: VENDAS POR FABRICANTE (Gráfico de Barras)
                # Só cria este gráfico se a coluna FABRICANTE existir (após o merge)
                if 'FABRICANTE' in dados_completos_df.columns:
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
                    grafico_fabricantes_html = "<div class='alert alert-warning'>Não foi possível gerar o gráfico por fabricante. Verifique o catálogo.</div>"


                # GRÁFICO 3: VENDAS POR DEPARTAMENTO E ITEM (Treemap)
                # Só cria este gráfico se as colunas DEPARTAMENTO e ITENS existirem
                if 'DEPARTAMENTO' in dados_completos_df.columns and 'ITENS' in dados_completos_df.columns:
                    # Remove linhas onde o departamento é nulo para não poluir o gráfico
                    df_treemap = dados_completos_df.dropna(subset=['DEPARTAMENTO'])
                    fig_treemap = px.treemap(
                        df_treemap,
                        path=[px.Constant("Todos Departamentos"), 'DEPARTAMENTO', 'ITENS'],
                        values='VENDA',
                        title='Vendas por Departamento e Itens (clique para explorar)'
                    )
                    grafico_treemap_html = fig_treemap.to_html(full_html=False)
                else:
                    grafico_treemap_html = "<div class='alert alert-warning'>Não foi possível gerar o gráfico por departamento. Verifique o catálogo.</div>"


                # 6. Envia os gráficos para a página de dashboard
                return render_template(
                    'dashboard.html', 
                    grafico1_html=fig_top_itens.to_html(full_html=False), 
                    grafico2_html=grafico_fabricantes_html,
                    grafico3_html=grafico_treemap_html # Enviando o novo gráfico
                )

            except Exception as e:
                flash(f'Erro ao processar o arquivo: {e}')
                return render_template('upload.html')
        else:
            flash('Formato de arquivo inválido. Por favor, envie um arquivo .xlsx ou .xls')
            return render_template('upload.html')

    return render_template('upload.html')