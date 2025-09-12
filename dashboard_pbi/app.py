# dashboard_pbi/app.py --- VERSÃO FINAL COM DEBUG DE TAMANHO DO DATAFRAME

import pandas as pd
import plotly.express as px
from flask import Flask, request, render_template, flash
import os
import io
from datetime import datetime

app = Flask(__name__)
app.secret_key = 'chave_final_correcao_colunas_v3' # Chave secreta atualizada

DEBUG_MODE = False 
DEBUG_FOLDER = 'debug_files'
if not os.path.exists(DEBUG_FOLDER):
    os.makedirs(DEBUG_FOLDER)

@app.route('/', methods=['GET', 'POST'])
def dashboard():
    if request.method == 'GET':
        return render_template('dashboard.html', resultados=None)

    if request.method == 'POST':
        file = request.files.get('file')
        if not file or file.filename == '':
            flash('Nenhum arquivo selecionado.', 'warning')
            return render_template('dashboard.html', resultados=None)

        try:
            file_content = file.read()
            if DEBUG_MODE:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                debug_filepath = os.path.join(DEBUG_FOLDER, f"uploaded_file_{timestamp}.xlsx")
                with open(debug_filepath, 'wb') as f:
                    f.write(file_content)
                flash(f"DEBUG: Arquivo salvo em {debug_filepath}", 'info')
            
            df = pd.read_excel(io.BytesIO(file_content), engine='openpyxl', header=0) # Mantemos header=0
            df.columns = df.columns.str.strip().str.upper()

            colunas_necessarias_do_arquivo = ['DESCRICAO PRODUTO', 'VALOR TOTAL ITEM', 'FABRICANTE']
            
            if DEBUG_MODE:
                flash(f"DEBUG: Colunas encontradas no arquivo: {df.columns.tolist()}", 'info')
                if not df.empty:
                    flash(f"DEBUG: Primeiras 5 linhas do arquivo (após leitura):\n{df.head().to_string()}", 'info')
                else:
                    flash("DEBUG: O arquivo foi lido, mas está vazio ANTES de qualquer processamento.", 'info')

            if not set(colunas_necessarias_do_arquivo).issubset(df.columns):
                # Este é o erro principal que queremos diagnosticar
                flash(f"ERRO: O relatório enviado não contém as colunas obrigatórias: {colunas_necessarias_do_arquivo}. "
                      f"Colunas encontradas: {df.columns.tolist()}", 'danger')
                return render_template('dashboard.html', resultados=None)
            
            # --- NOVO DEBUG: Verificação após leitura, antes da limpeza ---
            if df.empty:
                flash("ERRO: O arquivo foi lido, as colunas foram encontradas, mas o DataFrame está vazio.", 'danger')
                return render_template('dashboard.html', resultados=None)

            df.rename(columns={
                'DESCRICAO PRODUTO': 'ITENS', 
                'VALOR TOTAL ITEM': 'VENDA'
            }, inplace=True)

            # DEBUG: Tamanho antes da limpeza
            if DEBUG_MODE:
                flash(f"DEBUG: DF antes da limpeza (linhas): {len(df)}", 'info')

            df['VENDA'] = pd.to_numeric(df['VENDA'], errors='coerce').fillna(0)
            df.dropna(subset=['ITENS', 'FABRICANTE'], inplace=True) # remove linhas onde ITENS ou FABRICANTE são NaN
            df = df[df['VENDA'] > 0].copy() # remove linhas onde VENDA é 0 ou menor

            # DEBUG: Tamanho depois da limpeza
            if DEBUG_MODE:
                flash(f"DEBUG: DF depois da limpeza (linhas): {len(df)}", 'info')

            if df.empty:
                flash("Nenhum dado válido encontrado no relatório após a limpeza. Isso pode indicar que as colunas 'ITENS', 'FABRICANTE' ou 'VENDA' estão vazias na maioria das linhas, ou os valores de venda são 0. Por favor, verifique os dados.", 'info')
                return render_template('dashboard.html', resultados=None)

            top_10_itens = df.groupby('ITENS')['VENDA'].sum().nlargest(10).sort_values(ascending=True)
            fig_top_itens = px.bar(
                top_10_itens, x='VENDA', y=top_10_itens.index,
                orientation='h', title='Top 10 Itens Mais Vendidos', text_auto='.2s'
            )
            
            vendas_por_fabricante = df.groupby('FABRICANTE')['VENDA'].sum().nlargest(15).sort_values(ascending=False)
            fig_fabricantes = px.bar(
                vendas_por_fabricante, x=vendas_por_fabricante.index, y='VENDA',
                title='Top 15 Fabricantes por Venda', text_auto='.2s'
            )
            fig_fabricantes.update_layout(xaxis_tickangle=-45)
            
            resultados = {
                "grafico_top_itens": fig_top_itens.to_html(full_html=False),
                "grafico_fabricantes": fig_fabricantes.to_html(full_html=False)
            }
            
            return render_template('dashboard.html', resultados=resultados)

        except Exception as e:
            flash(f'Erro inesperado ao processar o arquivo: {e}. Verifique o formato do arquivo e suas colunas.', 'danger')
            return render_template('dashboard.html', resultados=None)