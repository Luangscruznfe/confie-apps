# dashboard_pbi/app.py - Versão Final com Leitor Universal

import pandas as pd
import plotly.express as px
from flask import Flask, request, render_template, flash, session, redirect, url_for
import os
import io # Necessário para ler os bytes do arquivo

app = Flask(__name__)
app.secret_key = 'sua_chave_secreta_aqui_flask_final'

def processar_dados(arquivo_bytes):
    """
    Tenta ler os dados do arquivo em múltiplos formatos (Excel, CSV com ';', CSV com ',').
    Esta função é um 'leitor universal' para aumentar a robustez.
    """
    # Tenta ler como Excel primeiro (formato principal esperado)
    try:
        df = pd.read_excel(io.BytesIO(arquivo_bytes), engine='openpyxl')
        print("INFO: Arquivo lido com sucesso como Excel (.xlsx).")
    except Exception as e_excel:
        print(f"AVISO: Falha ao ler como Excel ({e_excel}). Tentando como CSV.")
        # Se falhar, tenta ler como CSV com separador ';'
        try:
            # Precisamos 'rebobinar' o stream de bytes para uma nova tentativa de leitura
            arquivo_bytes.seek(0)
            df = pd.read_csv(io.BytesIO(arquivo_bytes), sep=';', encoding='latin-1')
            print("INFO: Arquivo lido com sucesso como CSV com separador ';'.")
        except Exception as e_csv_semicolon:
            print(f"AVISO: Falha ao ler como CSV com ';' ({e_csv_semicolon}). Tentando com ','.")
            # Se falhar, tenta ler como CSV com separador ','
            try:
                arquivo_bytes.seek(0)
                df = pd.read_csv(io.BytesIO(arquivo_bytes), sep=',', encoding='latin-1')
                print("INFO: Arquivo lido com sucesso como CSV com separador ','.")
            except Exception as e_csv_comma:
                # Se tudo falhar, levanta um erro claro
                print(f"ERRO: Falha em todas as tentativas de leitura. Último erro: {e_csv_comma}")
                raise ValueError("Formato de arquivo não reconhecido. Use .xlsx ou CSV (separado por ; ou ,).")

    df.columns = df.columns.str.strip().str.upper()
    
    colunas_necessarias = ['ITENS', 'VENDA', 'FABRICANTE', 'VENDEDOR']
    if not set(colunas_necessarias).issubset(df.columns):
        raise ValueError(f"ERRO: O relatório não contém as colunas obrigatórias: {colunas_necessarias}.")
    
    df['VENDA'] = pd.to_numeric(df['VENDA'], errors='coerce').fillna(0)
    df.dropna(subset=['ITENS', 'FABRICANTE', 'VENDEDOR'], inplace=True)
    df = df[df['VENDA'] > 0].copy()
    
    return df

@app.route('/', methods=['GET', 'POST'])
def upload_page():
    if request.method == 'POST':
        file = request.files.get('file')
        if not file or file.filename == '':
            flash('Nenhum arquivo selecionado.')
            return redirect(request.url)
        
        try:
            # Armazena os bytes do arquivo na sessão. Usamos um objeto BytesIO para ser mais flexível.
            session['uploaded_file'] = io.BytesIO(file.read()).getvalue()
            return redirect(url_for('dashboard_page'))
        except Exception as e:
            flash(f"Erro ao carregar o arquivo: {e}")
            return redirect(request.url)
            
    return render_template('upload.html')

@app.route('/dashboard')
def dashboard_page():
    if 'uploaded_file' not in session:
        return redirect(url_for('upload_page'))
        
    try:
        arquivo_bytes = io.BytesIO(session['uploaded_file'])
        df_filtrado = processar_dados(arquivo_bytes)

        # A lógica de filtros e gráficos continua a mesma
        opcoes_vendedor = sorted(df_filtrado['VENDEDOR'].unique())
        opcoes_fabricante = sorted(df_filtrado['FABRICANTE'].unique())

        vendedores_selecionados = request.args.getlist('vendedor')
        fabricantes_selecionados = request.args.getlist('fabricante')

        if not vendedores_selecionados: vendedores_selecionados = opcoes_vendedor
        if not fabricantes_selecionados: fabricantes_selecionados = opcoes_fabricante
        
        df_filtrado = df_filtrado[
            df_filtrado['VENDEDOR'].isin(vendedores_selecionados) &
            df_filtrado['FABRICANTE'].isin(fabricantes_selecionados)
        ]

        grafico_fabricantes_html = ""
        if not df_filtrado.empty:
            vendas_fabricante = df_filtrado.groupby('FABRICANTE')['VENDA'].sum().nlargest(20).sort_values(ascending=False)
            fig_fab = px.bar(vendas_fabricante, x=vendas_fabricante.index, y='VENDA', title='Vendas por Fabricante', text_auto='.2s')
            grafico_fabricantes_html = fig_fab.to_html(full_html=False)

        tabela_html = df_filtrado.to_html(classes='table table-striped table-hover', index=False, table_id='tabela-dados')

        return render_template('dashboard.html',
                               grafico_fabricantes=grafico_fabricantes_html,
                               tabela_dados=tabela_html,
                               opcoes_vendedor=opcoes_vendedor,
                               opcoes_fabricante=opcoes_fabricante,
                               vendedores_selecionados=vendedores_selecionados,
                               fabricantes_selecionados=fabricantes_selecionados
                               )

    except Exception as e:
        flash(f"Erro ao processar os dados: {e}")
        return redirect(url_for('upload_page'))