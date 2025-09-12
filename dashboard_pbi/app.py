# dashboard_pbi/app.py - Versão Final em FLASK

import unicodedata
import pandas as pd
import plotly.express as px
from flask import Flask, request, render_template, flash
from fuzzywuzzy import fuzz, process
import os
import logging

# --- Configuração do App Flask ---
app = Flask(__name__)
app.secret_key = 'sua_chave_secreta_aqui'

# --- Configuração do Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Funções de Lógica de Dados (A sua excelente lógica de fuzzy match) ---
def remover_acentos(s: str) -> str:
    if not isinstance(s, str): s = str(s)
    nfkd = unicodedata.normalize('NFKD', s)
    return "".join(ch for ch in nfkd if not unicodedata.combining(ch))

def normalizar_serie(serie: pd.Series) -> pd.Series:
    return serie.astype(str).str.strip().str.upper().apply(remover_acentos).str.replace(r'\s+', ' ', regex=True)

def encontrar_melhor_match(produto, catalogo_produtos, threshold=80):
    if not produto or pd.isna(produto): return None
    try:
        match = process.extractOne(produto, catalogo_produtos, scorer=fuzz.token_sort_ratio)
        return match if match and match[1] >= threshold else None
    except Exception as e:
        logger.error(f"Erro no match fuzzy para '{produto}': {e}")
        return None

def criar_mapeamento_produtos(df_vendas, catalogo_df, threshold=80):
    produtos_vendas = df_vendas['ITENS_NORM'].unique()
    produtos_catalogo = catalogo_df['DESCRICAO_NORM'].tolist()
    mapeamento, produtos_nao_encontrados = {}, []
    for produto in produtos_vendas:
        if pd.isna(produto) or produto == '': continue
        match_exato = catalogo_df[catalogo_df['DESCRICAO_NORM'] == produto]
        if not match_exato.empty:
            fabricante = match_exato.iloc[0]['FABRICANTE']
            mapeamento[produto] = (produto, fabricante, 100, 'EXATO')
        else:
            match_result = encontrar_melhor_match(produto, produtos_catalogo, threshold)
            if match_result:
                produto_encontrado, score = match_result
                fabricante_row = catalogo_df[catalogo_df['DESCRICAO_NORM'] == produto_encontrado]
                if not fabricante_row.empty:
                    fabricante = fabricante_row.iloc[0]['FABRICANTE']
                    mapeamento[produto] = (produto_encontrado, fabricante, score, 'FUZZY')
                else: produtos_nao_encontrados.append(produto)
            else: produtos_nao_encontrados.append(produto)
    return mapeamento, produtos_nao_encontrados

# --- Carregamento do Catálogo ---
catalogo_df = None
catalogo_erro = ""
try:
    APP_DIR = os.path.dirname(os.path.abspath(__file__))
    CATALOGO_PATH = os.path.join(APP_DIR, 'catalogo_produtos.xlsx')
    if os.path.exists(CATALOGO_PATH):
        catalogo_df = pd.read_excel(CATALOGO_PATH, engine='openpyxl')
        catalogo_df.columns = catalogo_df.columns.str.strip().str.upper()
        if {'DESCRICAO', 'FABRICANTE'}.issubset(catalogo_df.columns):
            catalogo_df['DESCRICAO_NORM'] = normalizar_serie(catalogo_df['DESCRICAO'])
            catalogo_df = catalogo_df.drop_duplicates(subset=['DESCRICAO_NORM']).reset_index(drop=True)
            logger.info("Catálogo carregado com sucesso.")
        else:
            catalogo_erro = "Colunas 'DESCRICAO' e 'FABRICANTE' são necessárias no catálogo."
            catalogo_df = None
    else:
        catalogo_erro = f"Arquivo {CATALOGO_PATH} não encontrado."
except Exception as e:
    catalogo_erro = f"Erro ao carregar catálogo: {e}"
    catalogo_df = None

if catalogo_erro: logger.error(catalogo_erro)

# --- Rota Principal do Flask ---
@app.route('/', methods=['GET', 'POST'])
def upload_analise():
    if catalogo_df is None:
        flash(f'Erro Crítico no Catálogo: {catalogo_erro}. A aplicação não pode continuar.')
        return render_template('upload.html')

    if request.method == 'POST':
        file = request.files.get('file')
        if not file or file.filename == '':
            flash('Nenhum arquivo selecionado.')
            return render_template('upload.html')

        try:
            df = pd.read_excel(file, engine='openpyxl')
            df.columns = df.columns.str.strip().str.upper()

            if not {'ITENS', 'VENDA'}.issubset(df.columns):
                flash("O relatório precisa ter as colunas 'ITENS' e 'VENDA'.")
                return render_template('upload.html')

            df['ITENS_NORM'] = normalizar_serie(df['ITENS'])
            df['VENDA'] = pd.to_numeric(df['VENDA'], errors='coerce').fillna(0)
            df = df[(df['VENDA'] > 0) & (df['ITENS_NORM'].str.len() > 0)].copy()

            if df.empty:
                flash('Nenhum dado válido encontrado no relatório após a limpeza.')
                return render_template('upload.html')

            threshold = int(request.form.get('threshold', 80))
            mapeamento, produtos_nao_encontrados = criar_mapeamento_produtos(df, catalogo_df, threshold)
            
            df['FABRICANTE_FINAL'] = None
            df['PRODUTO_CATALOGO'] = None
            df['MATCH_SCORE'] = None
            df['MATCH_TIPO'] = None

            for produto_venda, group in df.groupby('ITENS_NORM'):
                if produto_venda in mapeamento:
                    produto_cat, fabricante, score, tipo = mapeamento[produto_venda]
                    mask = df['ITENS_NORM'] == produto_venda
                    df.loc[mask, 'FABRICANTE_FINAL'] = fabricante
                    df.loc[mask, 'PRODUTO_CATALOGO'] = produto_cat
                    df.loc[mask, 'MATCH_SCORE'] = score
                    df.loc[mask, 'MATCH_TIPO'] = tipo
            
            # Geração dos Gráficos e Tabelas
            top_itens_df = df.groupby('ITENS')['VENDA'].sum().reset_index().nlargest(10, 'VENDA').sort_values('VENDA', ascending=True)
            fig_top_itens = px.bar(top_itens_df, x='VENDA', y='ITENS', orientation='h', title='Top 10 Itens Mais Vendidos', text_auto='.2s', color='VENDA', color_continuous_scale='Blues')
            
            df_fab = df.dropna(subset=['FABRICANTE_FINAL'])
            if df_fab.empty:
                grafico_fabricantes_html = "<div class='alert alert-warning'>Nenhum fabricante encontrado.</div>"
            else:
                fab_df = df_fab.groupby('FABRICANTE_FINAL')['VENDA'].sum().reset_index().nlargest(15, 'VENDA').sort_values('VENDA', ascending=False)
                fig_fabricantes = px.bar(fab_df, x='FABRICANTE_FINAL', y='VENDA', title='Top 15 Fabricantes por Venda', text_auto='.2s', color='VENDA', color_continuous_scale='Greens')
                fig_fabricantes.update_layout(xaxis_tickangle=-45)
                grafico_fabricantes_html = fig_fabricantes.to_html(full_html=False)

            # ... (código para as tabelas e métricas) ...

            return render_template('dashboard.html',
                                   grafico_top_itens=fig_top_itens.to_html(full_html=False),
                                   grafico_fabricantes=grafico_fabricantes_html,
                                   # ... (outras variáveis para o template) ...
                                   )
        except Exception as e:
            flash(f'Erro ao processar arquivo: {str(e)}')
            logger.error(f"Erro no processamento: {e}", exc_info=True)
            return render_template('upload.html')
    
    return render_template('upload.html')