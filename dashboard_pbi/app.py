# app.py (trechos principais)

import unicodedata
import pandas as pd
import plotly.express as px
from flask import Flask, request, render_template, flash

app = Flask(__name__)

# --- Normalização de texto ---
def remover_acentos(s: str) -> str:
    if not isinstance(s, str):
        s = str(s)
    nfkd = unicodedata.normalize('NFKD', s)
    return ''.join(ch for ch in nfkd if not unicodedata.combining(ch))

def normalizar_serie(serie: pd.Series) -> pd.Series:
    return (
        serie.astype(str)
             .str.strip()
             .str.upper()
             .map(remover_acentos)
             .str.replace(r'\s+', ' ', regex=True)
    )

# --- Carrega e prepara o catálogo uma vez ---
CATALOGO_PATH = 'catalogo_produtos.xlsx'
catalogo_df = pd.read_excel(CATALOGO_PATH, engine='openpyxl')
# Garanta que as colunas existam e padronize nomes
catalogo_df.columns = catalogo_df.columns.str.strip().str.upper()
# Supondo colunas: DESCRICAO e FABRICANTE
catalogo_df['DESCRICAO_NORM'] = normalizar_serie(catalogo_df['DESCRICAO'])
# Deduplica para evitar multiplicação de linhas no merge
catalogo_df = catalogo_df.drop_duplicates(subset=['DESCRICAO_NORM']).reset_index(drop=True)

@app.route('/', methods=['GET', 'POST'])
def upload_analise():
    if request.method == 'POST':
        file = request.files.get('file')
        if not file or file.filename == '':
            flash('Nenhum arquivo selecionado.')
            return render_template('upload.html')

        # Lê relatório de vendas
        df = pd.read_excel(file, engine='openpyxl')
        df.columns = df.columns.str.strip().str.upper()

        # Validação mínima
        if not {'ITENS', 'VENDA'}.issubset(df.columns):
            flash("O relatório precisa ter as colunas 'ITENS' e 'VENDA'.")
            return render_template('upload.html')

        # Normaliza chaves
        df['ITENS_NORM'] = normalizar_serie(df['ITENS'])
        df['VENDA'] = pd.to_numeric(df['VENDA'], errors='coerce').fillna(0)

        # Merge por descrição normalizada
        dados = pd.merge(
            df,
            catalogo_df[['DESCRICAO_NORM', 'FABRICANTE']],  # pegue só o necessário
            left_on='ITENS_NORM',
            right_on='DESCRICAO_NORM',
            how='left'
        )

        # Coluna unificada de fabricante
        dados['FABRICANTE_FINAL'] = dados['FABRICANTE']

        # --------- Gráfico 1: Top 10 Itens ----------
        top_itens_df = (
            dados.groupby('ITENS', as_index=False)['VENDA']
                 .sum()
                 .nlargest(10, 'VENDA')
                 .sort_values('VENDA', ascending=True)
        )
        fig_top_itens = px.bar(
            top_itens_df, x='VENDA', y='ITENS',
            orientation='h',
            title='Top 10 Itens Mais Vendidos',
            text_auto='.2s'
        )

        # --------- Gráfico 2: Top 15 Fabricantes ----------
        df_fab = dados.dropna(subset=['FABRICANTE_FINAL'])
        if df_fab.empty:
            grafico_fabricantes_html = "<div class='alert alert-info'>Nenhum fabricante encontrado no match.</div>"
        else:
            fab_df = (
                df_fab.groupby('FABRICANTE_FINAL', as_index=False)['VENDA']
                      .sum()
                      .nlargest(15, 'VENDA')
                      .sort_values('VENDA', ascending=False)
            )
            fig_fabricantes = px.bar(
                fab_df, x='FABRICANTE_FINAL', y='VENDA',
                title='Top 15 Fabricantes por Venda',
                text_auto='.2s'
            )
            grafico_fabricantes_html = fig_fabricantes.to_html(full_html=False)

        # --------- Tabela de conferência ----------
        tabela_conf_df = (
            dados.groupby(['ITENS', 'FABRICANTE_FINAL'], as_index=False)['VENDA']
                 .sum()
                 .sort_values('VENDA', ascending=False)
                 .head(50)
        )
        tabela_conf_html = tabela_conf_df.to_html(index=False, classes="table table-striped table-sm")

        # Métricas de match
        itens_total = dados['ITENS'].nunique()
        itens_casados = dados.dropna(subset=['FABRICANTE_FINAL'])['ITENS'].nunique()
        match_rate = (itens_casados / itens_total * 100) if itens_total else 0
        resumo_match_html = f"""
            <div class='alert alert-secondary'>
                Itens únicos: <strong>{itens_total}</strong> |
                Itens com fabricante: <strong>{itens_casados}</strong> |
                Taxa de match: <strong>{match_rate:.1f}%</strong>
            </div>
        """

        return render_template(
            'dashboard.html',
            grafico1_html=fig_top_itens.to_html(full_html=False),
            grafico2_html=grafico_fabricantes_html,
            tabela_conf_html=tabela_conf_html,
            resumo_match_html=resumo_match_html
        )

    return render_template('upload.html')