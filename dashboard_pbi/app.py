# dashboard_pbi/app.py --- VERSÃO COM CORREÇÃO DE LÓGICA DE FILTRO

import pandas as pd
import plotly.express as px
from flask import Flask, request, render_template, flash, session, redirect, url_for
import os
import uuid

app = Flask(__name__)
app.secret_key = 'chave_secreta_para_filtros_funcionais'

UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


@app.route('/', methods=['GET', 'POST'])
def dashboard():
    if request.method == 'POST':
        # --- LÓGICA DE UPLOAD ---

        # 1. Limpa qualquer arquivo antigo antes de processar um novo
        if 'uploaded_filename' in session:
            old_filepath = os.path.join(UPLOAD_FOLDER, session['uploaded_filename'])
            if os.path.exists(old_filepath):
                os.remove(old_filepath)
            session.pop('uploaded_filename', None)

        file = request.files.get('file')
        if not file or file.filename == '':
            flash('Nenhum arquivo selecionado.', 'warning')
            return redirect(request.url)

        try:
            filename = str(uuid.uuid4()) + ".xlsx"
            filepath = os.path.join(UPLOAD_FOLDER, filename)
            file.save(filepath)
            session['uploaded_filename'] = filename
            flash('Arquivo carregado com sucesso!', 'success')
            # Redireciona para a própria página via GET para mostrar os resultados iniciais
            return redirect(url_for('dashboard'))

        except Exception as e:
            flash(f'Erro ao salvar o arquivo: {e}.', 'danger')
            return redirect(request.url)

    # --- LÓGICA DE EXIBIÇÃO E FILTRO (GET) ---
    resultados = None
    if 'uploaded_filename' in session:
        filename = session['uploaded_filename']
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        
        if not os.path.exists(filepath):
            flash('Arquivo de sessão não encontrado. Por favor, carregue novamente.', 'warning')
            session.pop('uploaded_filename', None)
            return redirect(url_for('dashboard'))
            
        try:
            df = pd.read_excel(filepath, engine='openpyxl', header=0)
            df.columns = df.columns.str.strip().str.upper()

            # (O restante do código de processamento permanece o mesmo)
            colunas_necessarias = [
                'DESCRICAO PRODUTO', 'VALOR TOTAL ITEM', 'FABRICANTE', 'VENDEDOR',
                'CATEGORIA PRODUTO', 'CIDADE', 'SEGMENTO CLIENTE'
            ]
            
            if not set(colunas_necessarias).issubset(df.columns):
                flash(f"ERRO: Colunas obrigatórias não encontradas.", 'danger')
                return redirect(url_for('dashboard'))

            df.rename(columns={'DESCRICAO PRODUTO': 'PRODUTO', 'VALOR TOTAL ITEM': 'VENDA'}, inplace=True)
            df['VENDA'] = pd.to_numeric(df['VENDA'], errors='coerce').fillna(0)
            df.dropna(subset=['PRODUTO', 'FABRICANTE', 'VENDEDOR', 'CATEGORIA PRODUTO', 'CIDADE', 'SEGMENTO CLIENTE'], inplace=True)
            df = df[df['VENDA'] > 0].copy()

            if df.empty:
                flash("Nenhum dado válido encontrado no arquivo.", 'info')
                return render_template('dashboard.html', resultados=None)

            # Preparação das opções para os filtros
            opcoes_vendedor = sorted(df['VENDEDOR'].unique().tolist())
            opcoes_produto = sorted(df['PRODUTO'].unique().tolist())
            opcoes_fabricante = sorted(df['FABRICANTE'].unique().tolist())
            opcoes_categoria = sorted(df['CATEGORIA PRODUTO'].unique().tolist())
            opcoes_cidade = sorted(df['CIDADE'].unique().tolist())
            opcoes_segmento = sorted(df['SEGMENTO CLIENTE'].unique().tolist())

            # Aplicação dos filtros da URL (se existirem)
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
            
            if df_filtrado.empty:
                flash("Nenhum dado encontrado para os filtros selecionados.", 'info')

            # Geração de gráficos (mesmo que vazios, para manter a estrutura)
            grafico_top_itens_html = ""
            if not df_filtrado.empty:
                top_10_itens = df_filtrado.groupby('PRODUTO')['VENDA'].sum().nlargest(10).sort_values(ascending=True)
                fig_top_itens = px.bar(top_10_itens, x='VENDA', y=top_10_itens.index, orientation='h', title='Top 10 Itens Mais Vendidos (Filtrado)', text_auto='.2s')
                grafico_top_itens_html = fig_top_itens.to_html(full_html=False)
            
            grafico_fabricantes_html = ""
            if not df_filtrado.empty:
                vendas_por_fabricante = df_filtrado.groupby('FABRICANTE')['VENDA'].sum().nlargest(15).sort_values(ascending=False)
                fig_fabricantes = px.bar(vendas_por_fabricante, x=vendas_por_fabricante.index, y='VENDA', title='Top 15 Fabricantes por Venda (Filtrado)', text_auto='.2s')
                fig_fabricantes.update_layout(xaxis_tickangle=-45)
                grafico_fabricantes_html = fig_fabricantes.to_html(full_html=False)

            resultados = { "grafico_top_itens": grafico_top_itens_html, "grafico_fabricantes": grafico_fabricantes_html, "opcoes_vendedor": opcoes_vendedor, "selected_vendedores": selected_vendedores, "opcoes_produto": opcoes_produto, "selected_produtos": selected_produtos, "opcoes_fabricante": opcoes_fabricante, "selected_fabricantes": selected_fabricantes, "opcoes_categoria": opcoes_categoria, "selected_categorias": selected_categorias, "opcoes_cidade": opcoes_cidade, "selected_cidades": selected_cidades, "opcoes_segmento": opcoes_segmento, "selected_segmentos": selected_segmentos }
            
        except Exception as e:
            flash(f'Erro ao processar o arquivo: {e}.', 'danger')
            return redirect(url_for('dashboard'))
            
    return render_template('dashboard.html', resultados=resultados)