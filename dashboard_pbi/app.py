# dashboard_pbi/app.py - Versão Flask com Filtros e Tabela Interativa

import pandas as pd
import plotly.express as px
from flask import Flask, request, render_template, flash, session, redirect, url_for
import os

app = Flask(__name__)
app.secret_key = 'sua_chave_secreta_aqui_flask_final'

# Função para carregar e processar os dados
def processar_dados(arquivo_bytes):
    df = pd.read_excel(arquivo_bytes, engine='openpyxl')
    df.columns = df.columns.str.strip().str.upper()
    
    # Validação de colunas essenciais
    colunas_necessarias = ['ITENS', 'VENDA', 'FABRICANTE', 'VENDEDOR'] # Adicione outras se necessário
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
            # Armazena os bytes do arquivo na sessão do usuário
            session['uploaded_file'] = file.read()
            # Redireciona para a página do dashboard
            return redirect(url_for('dashboard_page'))
        except Exception as e:
            flash(f"Erro ao ler o arquivo: {e}")
            return redirect(request.url)
            
    return render_template('upload.html')

@app.route('/dashboard')
def dashboard_page():
    if 'uploaded_file' not in session:
        return redirect(url_for('upload_page'))
        
    try:
        # Lê os dados do arquivo armazenado na sessão
        arquivo_bytes = session['uploaded_file']
        df = processar_dados(arquivo_bytes)

        # --- Lógica dos Filtros ---
        # Pega as opções de filtro da tabela COMPLETA
        opcoes_vendedor = sorted(df['VENDEDOR'].unique())
        opcoes_fabricante = sorted(df['FABRICANTE'].unique())

        # Pega os valores selecionados pelo usuário (da URL)
        vendedores_selecionados = request.args.getlist('vendedor')
        fabricantes_selecionados = request.args.getlist('fabricante')

        # Se nenhum filtro foi selecionado, seleciona todos por padrão
        if not vendedores_selecionados: vendedores_selecionados = opcoes_vendedor
        if not fabricantes_selecionados: fabricantes_selecionados = opcoes_fabricante
        
        # Aplica os filtros na tabela
        df_filtrado = df[
            df['VENDEDOR'].isin(vendedores_selecionados) &
            df['FABRICANTE'].isin(fabricantes_selecionados)
        ]

        # --- Geração dos Gráficos (com dados filtrados) ---
        grafico_fabricantes_html = ""
        if not df_filtrado.empty:
            vendas_fabricante = df_filtrado.groupby('FABRICANTE')['VENDA'].sum().nlargest(20).sort_values(ascending=False)
            fig_fab = px.bar(vendas_fabricante, x=vendas_fabricante.index, y='VENDA', title='Vendas por Fabricante', text_auto='.2s')
            grafico_fabricantes_html = fig_fab.to_html(full_html=False)

        # --- Geração da Tabela HTML (com dados filtrados) ---
        tabela_html = df_filtrado.to_html(classes='table table-striped table-hover', index=False, table_id='tabela-dados')

        return render_template('dashboard.html',
                               grafico_fabricantes=grafico_fabricantes_html,
                               tabela_dados=tabela_html,
                               # Passa as opções e seleções para os filtros
                               opcoes_vendedor=opcoes_vendedor,
                               opcoes_fabricante=opcoes_fabricante,
                               vendedores_selecionados=vendedores_selecionados,
                               fabricantes_selecionados=fabricantes_selecionados
                               )

    except Exception as e:
        flash(f"Erro ao processar os dados: {e}")
        return redirect(url_for('upload_page'))