# dashboard_pbi/app.py --- VERSÃO COM LOGS DE DIAGNÓSTICO

import pandas as pd
import plotly.express as px
from flask import Flask, request, render_template, flash, session, redirect, url_for
import os
import io
import base64

app = Flask(__name__)
app.secret_key = 'chave_final_com_filtros'

@app.route('/', methods=['GET', 'POST'])
def dashboard():
    print("DEBUG: Acessando a rota '/'")
    resultados = None

    if request.method == 'POST':
        print("DEBUG: Método é POST, iniciando upload.")
        file = request.files.get('file')
        if not file or file.filename == '':
            flash('Nenhum arquivo selecionado.', 'warning')
            return render_template('dashboard.html', resultados=None)

        try:
            file_content = file.read()
            session['uploaded_file_content'] = base64.b64encode(file_content).decode('utf-8')
            flash('Arquivo carregado com sucesso! Use os filtros abaixo.', 'success')
            print("DEBUG: Arquivo salvo na sessão, redirecionando.")
            return redirect(url_for('dashboard'))

        except Exception as e:
            print(f"ERRO no upload: {e}")
            flash(f'Erro inesperado ao carregar o arquivo: {e}.', 'danger')
            return render_template('dashboard.html', resultados=None)

    if 'uploaded_file_content' in session:
        try:
            print("DEBUG: 1. Encontrou arquivo na sessão. Decodificando...")
            file_content_decoded = base64.b64decode(session['uploaded_file_content'])
            
            print("DEBUG: 2. Lendo o arquivo Excel com pandas...")
            df = pd.read_excel(io.BytesIO(file_content_decoded), engine='openpyxl', header=0)
            print("DEBUG: 3. Arquivo lido para o DataFrame com sucesso.")
            
            df.columns = df.columns.str.strip().str.upper()

            colunas_necessarias_do_arquivo = [
                'DESCRICAO PRODUTO', 'VALOR TOTAL ITEM', 'FABRICANTE', 'VENDEDOR',
                'CATEGORIA PRODUTO', 'CIDADE', 'SEGMENTO CLIENTE'
            ]
            
            if not set(colunas_necessarias_do_arquivo).issubset(df.columns):
                flash(f"ERRO: Colunas obrigatórias não encontradas.", 'danger')
                del session['uploaded_file_content']
                return render_template('dashboard.html', resultados=None)

            print("DEBUG: 4. Renomeando e limpando os dados...")
            df.rename(columns={
                'DESCRICAO PRODUTO': 'PRODUTO',
                'VALOR TOTAL ITEM': 'VENDA'
            }, inplace=True)

            df['VENDA'] = pd.to_numeric(df['VENDA'], errors='coerce').fillna(0)
            df.dropna(subset=['PRODUTO', 'FABRICANTE', 'VENDEDOR', 'CATEGORIA PRODUTO', 'CIDADE', 'SEGMENTO CLIENTE'], inplace=True)
            df = df[df['VENDA'] > 0].copy()
            print("DEBUG: 5. Dados limpos.")

            if df.empty:
                flash("Nenhum dado válido encontrado após a limpeza.", 'info')
                del session['uploaded_file_content']
                return render_template('dashboard.html', resultados=None)

            print("DEBUG: 6. Preparando opções para os filtros...")
            opcoes_vendedor = sorted(df['VENDEDOR'].unique().tolist())
            opcoes_produto = sorted(df['PRODUTO'].unique().tolist())
            opcoes_fabricante = sorted(df['FABRICANTE'].unique().tolist())
            opcoes_categoria = sorted(df['CATEGORIA PRODUTO'].unique().tolist())
            opcoes_cidade = sorted(df['CIDADE'].unique().tolist())
            opcoes_segmento = sorted(df['SEGMENTO CLIENTE'].unique().tolist())
            print("DEBUG: 7. Opções de filtro prontas. Aplicando filtros selecionados...")

            selected_vendedores = request.args.getlist('vendedor') or opcoes_vendedor
            selected_produtos = request.args.getlist('produto') or opcoes_produto
            selected_fabricantes = request.args.getlist('fabricante') or opcoes_fabricante
            selected_categorias = request.args.getlist('categoria') or opcoes_categoria
            selected_cidades = request.args.getlist('cidade') or opcoes_cidade
            selected_segmentos = request.args.getlist('segmento') or opcoes_segmento

            df_filtrado = df[
                df['VENDEDOR'].isin(selected_vendedores) &
                df['PRODUTO'].isin(selected_produtos) &
                df['FABRICANTE'].isin(selected_fabricantes) &
                df['CATEGORIA PRODUTO'].isin(selected_categorias) &
                df['CIDADE'].isin(selected_cidades) &
                df['SEGMENTO CLIENTE'].isin(selected_segmentos)
            ]
            print("DEBUG: 8. DataFrame filtrado.")

            if df_filtrado.empty:
                flash("Nenhum dado encontrado para os filtros selecionados.", 'info')

            print("DEBUG: 9. Gerando gráfico de Top Itens...")
            grafico_top_itens_html = ""
            if not df_filtrado.empty:
                top_10_itens = df_filtrado.groupby('PRODUTO')['VENDA'].sum().nlargest(10).sort_values(ascending=True)
                fig_top_itens = px.bar(top_10_itens, x='VENDA', y=top_10_itens.index, orientation='h', title='Top 10 Itens Mais Vendidos', text_auto='.2s')
                grafico_top_itens_html = fig_top_itens.to_html(full_html=False)
            print("DEBUG: 10. Gráfico de Top Itens gerado.")
            
            print("DEBUG: 11. Gerando gráfico de Fabricantes...")
            grafico_fabricantes_html = ""
            if not df_filtrado.empty:
                vendas_por_fabricante = df_filtrado.groupby('FABRICANTE')['VENDA'].sum().nlargest(15).sort_values(ascending=False)
                fig_fabricantes = px.bar(vendas_por_fabricante, x=vendas_por_fabricante.index, y='VENDA', title='Top 15 Fabricantes por Venda', text_auto='.2s')
                fig_fabricantes.update_layout(xaxis_tickangle=-45)
                grafico_fabricantes_html = fig_fabricantes.to_html(full_html=False)
            print("DEBUG: 12. Gráfico de Fabricantes gerado.")

            resultados = { "grafico_top_itens": grafico_top_itens_html, "grafico_fabricantes": grafico_fabricantes_html, "opcoes_vendedor": opcoes_vendedor, "selected_vendedores": selected_vendedores, "opcoes_produto": opcoes_produto, "selected_produtos": selected_produtos, "opcoes_fabricante": opcoes_fabricante, "selected_fabricantes": selected_fabricantes, "opcoes_categoria": opcoes_categoria, "selected_categorias": selected_categorias, "opcoes_cidade": opcoes_cidade, "selected_cidades": selected_cidades, "opcoes_segmento": opcoes_segmento, "selected_segmentos": selected_segmentos }
            
            print("DEBUG: 13. Preparando para renderizar o template final.")
            return render_template('dashboard.html', resultados=resultados)

        except Exception as e:
            print(f"ERRO no processamento: {e}")
            flash(f'Erro inesperado ao processar os dados: {e}.', 'danger')
            if 'uploaded_file_content' in session: del session['uploaded_file_content']
            return render_template('dashboard.html', resultados=None)
            
    return render_template('dashboard.html', resultados=None)