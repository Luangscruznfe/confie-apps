# dashboard_pbi/app.py --- VERSÃO COM TELA INICIAL SIMPLIFICADA

import pandas as pd
import plotly.express as px
from flask import Flask, request, render_template, flash, session, redirect, url_for
import os
import uuid

app = Flask(__name__)
app.secret_key = 'chave_fluxo_inicial_limpo'

UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

dataframe_cache = {}

def get_dataframe(filepath):
    if filepath in dataframe_cache:
        print("INFO: DataFrame retornado do CACHE.")
        return dataframe_cache[filepath]
    else:
        print("INFO: DataFrame lido do ARQUIVO e salvo no cache.")
        df = pd.read_excel(filepath, engine='openpyxl', header=0)
        dataframe_cache.clear()
        dataframe_cache[filepath] = df
        return df

@app.route('/', methods=['GET', 'POST'])
def dashboard():
    if request.method == 'POST':
        dataframe_cache.clear()
        if 'uploaded_filename' in session:
            old_filepath = os.path.join(UPLOAD_FOLDER, session['uploaded_filename'])
            if os.path.exists(old_filepath): os.remove(old_filepath)
        
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
            return redirect(url_for('dashboard'))
        except Exception as e:
            flash(f'Erro ao salvar o arquivo: {e}.', 'danger')
            return redirect(request.url)

    resultados = None
    if 'uploaded_filename' in session:
        filename = session['uploaded_filename']
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        
        if not os.path.exists(filepath):
            flash('Arquivo de sessão expirou. Por favor, carregue novamente.', 'warning')
            session.pop('uploaded_filename', None)
            return redirect(url_for('dashboard'))
            
        try:
            df_original = get_dataframe(filepath)
            df = df_original.copy()
            
            df.columns = df.columns.str.strip().str.upper()
            df.rename(columns={'DESCRICAO PRODUTO': 'PRODUTO', 'VALOR TOTAL ITEM': 'VENDA'}, inplace=True)
            df['VENDA'] = pd.to_numeric(df['VENDA'], errors='coerce').fillna(0)
            df.dropna(subset=['PRODUTO', 'FABRICANTE', 'VENDEDOR', 'CATEGORIA PRODUTO', 'CIDADE', 'SEGMENTO CLIENTE'], inplace=True)
            df = df[df['VENDA'] > 0]
            
            opcoes_vendedor = sorted(df['VENDEDOR'].unique().tolist())
            opcoes_produto = sorted(df['PRODUTO'].unique().tolist())
            opcoes_fabricante = sorted(df['FABRICANTE'].unique().tolist())
            opcoes_categoria = sorted(df['CATEGORIA PRODUTO'].unique().tolist())
            opcoes_cidade = sorted(df['CIDADE'].unique().tolist())
            opcoes_segmento = sorted(df['SEGMENTO CLIENTE'].unique().tolist())

            # --- NOVA LÓGICA DE EXIBIÇÃO ---
            # Verifica se algum filtro foi passado na URL
            filtros_aplicados = bool(request.args)
            
            resultados = {
                "opcoes_vendedor": opcoes_vendedor, "selected_vendedores": request.args.getlist('vendedor') or opcoes_vendedor,
                "opcoes_produto": opcoes_produto, "selected_produtos": request.args.getlist('produto') or opcoes_produto,
                "opcoes_fabricante": opcoes_fabricante, "selected_fabricantes": request.args.getlist('fabricante') or opcoes_fabricante,
                "opcoes_categoria": opcoes_categoria, "selected_categorias": request.args.getlist('categoria') or opcoes_categoria,
                "opcoes_cidade": opcoes_cidade, "selected_cidades": request.args.getlist('cidade') or opcoes_cidade,
                "opcoes_segmento": opcoes_segmento, "selected_segmentos": request.args.getlist('segmento') or opcoes_segmento
            }

            if not filtros_aplicados:
                # --- VISTA INICIAL ---
                resultados['view_mode'] = 'initial'
                vendas_por_vendedor = df.groupby('VENDEDOR')['VENDA'].sum().sort_values(ascending=False)
                fig_inicial = px.bar(
                    vendas_por_vendedor,
                    x=vendas_por_vendedor.index,
                    y=vendas_por_vendedor.values,
                    title='Total de Vendas por Vendedor',
                    text_auto='.2s'
                )
                fig_inicial.update_layout(xaxis_title="Vendedor", yaxis_title="Total de Vendas")
                resultados['grafico_inicial'] = fig_inicial.to_html(full_html=False)

            else:
                # --- VISTA FILTRADA (DETALHADA) ---
                resultados['view_mode'] = 'filtered'
                df_filtrado = df[
                    df['VENDEDOR'].isin(resultados['selected_vendedores']) &
                    df['PRODUTO'].isin(resultados['selected_produtos']) &
                    df['FABRICANTE'].isin(resultados['selected_fabricantes']) &
                    df['CATEGORIA PRODUTO'].isin(resultados['selected_categorias']) &
                    df['CIDADE'].isin(resultados['selected_cidades']) &
                    df['SEGMENTO CLIENTE'].isin(resultados['selected_segmentos'])
                ]

                if df_filtrado.empty:
                    flash("Nenhum dado encontrado para os filtros selecionados.", 'info')

                grafico_top_itens_html = ""
                if not df_filtrado.empty:
                    # ... (código do gráfico top_itens)
                    grafico_top_itens_html = px.bar(...).to_html(full_html=False)
                
                grafico_vendedor_fabricante_html = ""
                if not df_filtrado.empty:
                    # ... (código do gráfico vendedor/fabricante)
                    grafico_vendedor_fabricante_html = px.bar(...).to_html(full_html=False)
                    
                resultados['grafico_top_itens'] = grafico_top_itens_html
                resultados['grafico_vendedor_fabricante'] = grafico_vendedor_fabricante_html

        except Exception as e:
            flash(f'Erro ao processar os dados: {e}.', 'danger')
            return redirect(url_for('dashboard'))
            
    return render_template('dashboard.html', resultados=resultados)