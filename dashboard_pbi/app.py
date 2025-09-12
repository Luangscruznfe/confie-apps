# dashboard_pbi/app.py - Versão Final Unificada

import pandas as pd
import plotly.express as px
from flask import Flask, request, render_template, flash
import os
import io

app = Flask(__name__)
app.secret_key = 'sua_chave_secreta_aqui_final_unificada'

def processar_dados(arquivo_bytes):
    df = pd.read_excel(io.BytesIO(arquivo_bytes), engine='openpyxl')
    df.columns = df.columns.str.strip().str.upper()
    
    colunas_necessarias = ['ITENS', 'VENDA', 'FABRICANTE', 'VENDEDOR']
    if not set(colunas_necessarias).issubset(df.columns):
        raise ValueError(f"ERRO: O relatório não contém as colunas obrigatórias: {colunas_necessarias}.")
    
    df['VENDA'] = pd.to_numeric(df['VENDA'], errors='coerce').fillna(0)
    df.dropna(subset=['ITENS', 'FABRICANTE', 'VENDEDOR'], inplace=True)
    df = df[df['VENDA'] > 0].copy()
    
    return df

@app.route('/', methods=['GET', 'POST'])
def dashboard_unificado():
    if request.method == 'POST':
        file = request.files.get('file')
        if not file or file.filename == '':
            flash('Nenhum arquivo selecionado.')
            return render_template('dashboard_unificado.html', resultados=None)

        try:
            # Lê os bytes e processa os dados
            arquivo_bytes = io.BytesIO(file.read())
            df = processar_dados(arquivo_bytes)

            # Pega as opções de filtro da tabela COMPLETA
            opcoes_vendedor = sorted(df['VENDEDOR'].unique())
            opcoes_fabricante = sorted(df['FABRICANTE'].unique())

            # Pega os valores selecionados pelo usuário (do formulário)
            vendedores_selecionados = request.form.getlist('vendedor')
            fabricantes_selecionados = request.form.getlist('fabricante')

            # Se nenhum filtro foi selecionado, seleciona todos
            if not vendedores_selecionados: vendedores_selecionados = opcoes_vendedor
            if not fabricantes_selecionados: fabricantes_selecionados = opcoes_fabricante
            
            # Aplica os filtros na tabela
            df_filtrado = df[
                df['VENDEDOR'].isin(vendedores_selecionados) &
                df['FABRICANTE'].isin(fabricantes_selecionados)
            ]

            # Gera Gráfico
            grafico_fabricantes_html = "<div class='alert alert-info'>Nenhum dado para exibir com os filtros selecionados.</div>"
            if not df_filtrado.empty:
                vendas_fabricante = df_filtrado.groupby('FABRICANTE')['VENDA'].sum().nlargest(20).sort_values(ascending=False)
                fig_fab = px.bar(vendas_fabricante, x=vendas_fabricante.index, y='VENDA', title='Vendas por Fabricante', text_auto='.2s')
                grafico_fabricantes_html = fig_fab.to_html(full_html=False)

            # Gera Tabela HTML
            tabela_html = df_filtrado.to_html(classes='table table-striped table-hover', index=False, table_id='tabela-dados')

            # Agrupa todos os resultados para enviar ao template
            resultados = {
                "grafico_fabricantes": grafico_fabricantes_html,
                "tabela_dados": tabela_html,
                "opcoes_vendedor": opcoes_vendedor,
                "opcoes_fabricante": opcoes_fabricante,
                "vendedores_selecionados": vendedores_selecionados,
                "fabricantes_selecionados": fabricantes_selecionados
            }
            
            return render_template('dashboard_unificado.html', resultados=resultados)

        except Exception as e:
            flash(f"Erro ao processar o arquivo: {e}")
            return render_template('dashboard_unificado.html', resultados=None)
            
    # Se for GET, apenas mostra a página inicial com o formulário de upload
    return render_template('dashboard_unificado.html', resultados=None)