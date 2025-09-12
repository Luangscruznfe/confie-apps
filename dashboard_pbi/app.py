# dashboard_pbi/app.py --- VERSÃO CORRIGIDA

import pandas as pd
import plotly.express as px
# A CORREÇÃO ESTÁ AQUI: Adicionamos session, redirect e url_for
from flask import Flask, request, render_template, flash, session, redirect, url_for
import os
import io
from datetime import datetime
import base64

app = Flask(__name__)
app.secret_key = 'chave_final_com_filtros' # Chave secreta atualizada para esta versão

@app.route('/', methods=['GET', 'POST'])
def dashboard():
    resultados = None # Inicializa resultados como None

    if request.method == 'POST':
        # --- Processamento do Upload ---
        file = request.files.get('file')
        if not file or file.filename == '':
            flash('Nenhum arquivo selecionado.', 'warning')
            return render_template('dashboard.html', resultados=None)

        try:
            file_content = file.read()
            # Salva o conteúdo do arquivo na sessão
            session['uploaded_file_content'] = base64.b64encode(file_content).decode('utf-8')

            flash('Arquivo carregado com sucesso! Use os filtros abaixo.', 'success')
            # Redireciona para a mesma página para exibir o dashboard com o arquivo carregado
            return redirect(url_for('dashboard'))

        except Exception as e:
            flash(f'Erro inesperado ao carregar o arquivo: {e}. Verifique o formato do arquivo.', 'danger')
            return render_template('dashboard.html', resultados=None)

    # --- Lógica do Dashboard e Filtros (para GET requests ou após POST de arquivo) ---
    if 'uploaded_file_content' in session:
        try:
            file_content_decoded = base64.b64decode(session['uploaded_file_content'])
            df = pd.read_excel(io.BytesIO(file_content_decoded), engine='openpyxl', header=0)
            df.columns = df.columns.str.strip().str.upper()

            # Nomes das colunas no seu arquivo Excel
            coluna_produto_raw = 'DESCRICAO PRODUTO'
            coluna_venda_raw = 'VALOR TOTAL ITEM'
            coluna_fabricante_raw = 'FABRICANTE'
            coluna_vendedor_raw = 'VENDEDOR'
            coluna_categoria_raw = 'CATEGORIA PRODUTO'
            coluna_cidade_raw = 'CIDADE'
            coluna_segmento_raw = 'SEGMENTO CLIENTE'

            colunas_necessarias_do_arquivo = [
                coluna_produto_raw, coluna_venda_raw, coluna_fabricante_raw,
                coluna_vendedor_raw, coluna_categoria_raw, coluna_cidade_raw, coluna_segmento_raw
            ]
            
            # Validação de colunas
            if not set(colunas_necessarias_do_arquivo).issubset(df.columns):
                flash(f"ERRO: O relatório carregado não contém todas as colunas obrigatórias: {colunas_necessarias_do_arquivo}. Colunas encontradas: {df.columns.tolist()}", 'danger')
                del session['uploaded_file_content'] # Limpa o arquivo da sessão
                return render_template('dashboard.html', resultados=None)
            
            if df.empty:
                flash("ERRO: O arquivo foi lido, mas não contém nenhuma linha de dados válida.", 'danger')
                del session['uploaded_file_content']
                return render_template('dashboard.html', resultados=None)

            # Renomeia colunas para nomes internos padronizados
            df.rename(columns={
                coluna_produto_raw: 'PRODUTO',
                coluna_venda_raw: 'VENDA'
            }, inplace=True)

            # Limpeza e conversão
            df['VENDA'] = pd.to_numeric(df['VENDA'], errors='coerce').fillna(0)
            df.dropna(subset=['PRODUTO', 'FABRICANTE', 'VENDEDOR', 'CATEGORIA PRODUTO', 'CIDADE', 'SEGMENTO CLIENTE'], inplace=True)
            df = df[df['VENDA'] > 0].copy()

            if df.empty:
                flash("Nenhum dado válido encontrado após a limpeza. Verifique os dados no arquivo.", 'info')
                del session['uploaded_file_content']
                return render_template('dashboard.html', resultados=None)

            # --- Preparar opções para os filtros ---
            opcoes_vendedor = sorted(df['VENDEDOR'].unique().tolist())
            opcoes_produto = sorted(df['PRODUTO'].unique().tolist())
            opcoes_fabricante = sorted(df['FABRICANTE'].unique().tolist())
            opcoes_categoria = sorted(df['CATEGORIA PRODUTO'].unique().tolist())
            opcoes_cidade = sorted(df['CIDADE'].unique().tolist())
            opcoes_segmento = sorted(df['SEGMENTO CLIENTE'].unique().tolist())

            # --- Aplicação dos filtros do formulário (GET requests) ---
            df_filtrado = df.copy()

            # Coleta os filtros do request.args (URL)
            selected_vendedores = request.args.getlist('vendedor')
            selected_produtos = request.args.getlist('produto')
            selected_fabricantes = request.args.getlist('fabricante')
            selected_categorias = request.args.getlist('categoria')
            selected_cidades = request.args.getlist('cidade')
            selected_segmentos = request.args.getlist('segmento')

            # Se uma lista de filtros está vazia, usa todas as opções (não filtra por ela)
            if not selected_vendedores: selected_vendedores = opcoes_vendedor
            if not selected_produtos: selected_produtos = opcoes_produto
            if not selected_fabricantes: selected_fabricantes = opcoes_fabricante
            if not selected_categorias: selected_categorias = opcoes_categoria
            if not selected_cidades: selected_cidades = opcoes_cidade
            if not selected_segmentos: selected_segmentos = opcoes_segmento

            df_filtrado = df_filtrado[
                df_filtrado['VENDEDOR'].isin(selected_vendedores) &
                df_filtrado['PRODUTO'].isin(selected_produtos) &
                df_filtrado['FABRICANTE'].isin(selected_fabricantes) &
                df_filtrado['CATEGORIA PRODUTO'].isin(selected_categorias) &
                df_filtrado['CIDADE'].isin(selected_cidades) &
                df_filtrado['SEGMENTO CLIENTE'].isin(selected_segmentos)
            ]

            if df_filtrado.empty:
                flash("Nenhum dado encontrado para os filtros selecionados. Tente ajustar os filtros.", 'info')
                
            # --- Geração dos Gráficos (com dados filtrados) ---\
            grafico_top_itens_html = ""
            if not df_filtrado.empty:
                top_10_itens = df_filtrado.groupby('PRODUTO')['VENDA'].sum().nlargest(10).sort_values(ascending=True)
                fig_top_itens = px.bar(
                    top_10_itens, x='VENDA', y=top_10_itens.index,
                    orientation='h', title='Top 10 Itens Mais Vendidos', text_auto='.2s'
                )
                grafico_top_itens_html = fig_top_itens.to_html(full_html=False)
            
            grafico_fabricantes_html = ""
            if not df_filtrado.empty:
                vendas_por_fabricante = df_filtrado.groupby('FABRICANTE')['VENDA'].sum().nlargest(15).sort_values(ascending=False)
                fig_fabricantes = px.bar(
                    vendas_por_fabricante, x=vendas_por_fabricante.index, y='VENDA',
                    title='Top 15 Fabricantes por Venda', text_auto='.2s'
                )
                fig_fabricantes.update_layout(xaxis_tickangle=-45)
                grafico_fabricantes_html = fig_fabricantes.to_html(full_html=False)

            # Agrupa todos os resultados para enviar ao template
            resultados = {
                "grafico_top_itens": grafico_top_itens_html,
                "grafico_fabricantes": grafico_fabricantes_html,
                "opcoes_vendedor": opcoes_vendedor,
                "selected_vendedores": selected_vendedores,
                "opcoes_produto": opcoes_produto,
                "selected_produtos": selected_produtos,
                "opcoes_fabricante": opcoes_fabricante,
                "selected_fabricantes": selected_fabricantes,
                "opcoes_categoria": opcoes_categoria,
                "selected_categorias": selected_categorias,
                "opcoes_cidade": opcoes_cidade,
                "selected_cidades": selected_cidades,
                "opcoes_segmento": opcoes_segmento,
                "selected_segmentos": selected_segmentos
            }
            
            return render_template('dashboard.html', resultados=resultados)

        except Exception as e:
            flash(f'Erro inesperado ao processar os dados: {e}. Por favor, recarregue o arquivo.', 'danger')
            if 'uploaded_file_content' in session:
                del session['uploaded_file_content']
            return render_template('dashboard.html', resultados=None)
            
    # Se for GET e não houver arquivo na sessão
    return render_template('dashboard.html', resultados=None)