# app.py - Versão 100% Streamlit

import unicodedata
import pandas as pd
import plotly.express as px
import streamlit as st
from fuzzywuzzy import fuzz, process
import os
import logging

# --- Configuração da Página e Logging ---
st.set_page_config(layout="wide", page_title="Dashboard de Vendas")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Funções de Normalização e Match (sem alterações) ---
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
             .apply(remover_acentos)
             .str.replace(r'\s+', ' ', regex=True)
    )

def encontrar_melhor_match(produto, catalogo_produtos, threshold=80):
    if not produto or pd.isna(produto):
        return None
    try:
        match = process.extractOne(produto, catalogo_produtos, scorer=fuzz.token_sort_ratio)
        if match and match[1] >= threshold:
            return match
        return None
    except Exception as e:
        logger.error(f"Erro no match fuzzy para '{produto}': {e}")
        return None

def criar_mapeamento_produtos(df_vendas, catalogo_df, threshold=80):
    produtos_vendas = df_vendas['ITENS_NORM'].unique()
    produtos_catalogo = catalogo_df['DESCRICAO_NORM'].tolist()
    
    mapeamento = {}
    produtos_nao_encontrados = []
    
    for produto in produtos_vendas:
        if pd.isna(produto) or produto == '':
            continue
            
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
                else:
                    produtos_nao_encontrados.append(produto)
            else:
                produtos_nao_encontrados.append(produto)
    
    return mapeamento, produtos_nao_encontrados

# --- Carrega o catálogo com cache do Streamlit ---
@st.cache_data
def carregar_catalogo():
    APP_DIR = os.path.dirname(os.path.abspath(__file__))
    CATALOGO_PATH = os.path.join(APP_DIR, 'catalogo_produtos.xlsx')
    try:
        if not os.path.exists(CATALOGO_PATH):
            return None, f"Arquivo {CATALOGO_PATH} não encontrado"
        
        catalogo_df = pd.read_excel(CATALOGO_PATH, engine='openpyxl')
        catalogo_df.columns = catalogo_df.columns.str.strip().str.upper()
        
        colunas_necessarias = ['DESCRICAO', 'FABRICANTE']
        colunas_faltando = [col for col in colunas_necessarias if col not in catalogo_df.columns]
        
        if colunas_faltando:
            return None, f"Colunas faltando no catálogo: {colunas_faltando}"
        
        catalogo_df['DESCRICAO_NORM'] = normalizar_serie(catalogo_df['DESCRICAO'])
        catalogo_df = catalogo_df.drop_duplicates(subset=['DESCRICAO_NORM']).reset_index(drop=True)
        
        logger.info(f"Catálogo carregado com sucesso: {len(catalogo_df)} produtos únicos")
        return catalogo_df, None
        
    except Exception as e:
        return None, f"Erro ao carregar catálogo: {str(e)}"

# --- Interface Principal da Aplicação ---
st.title("Dashboard Interativo de Vendas")

catalogo_df, catalogo_erro = carregar_catalogo()

if catalogo_df is None:
    st.error(f"Erro Crítico ao Carregar o Catálogo de Produtos: {catalogo_erro}")
    st.stop()

# --- Barra Lateral (Sidebar) ---
with st.sidebar:
    st.header("Controles")
    uploaded_file = st.file_uploader("Selecione o relatório de vendas (.xlsx)", type=['xlsx'])
    threshold = st.slider("Nível de Confiança para Correspondência", min_value=0, max_value=100, value=80)

# --- Lógica Principal: Roda apenas se um arquivo for enviado ---
if uploaded_file is not None:
    try:
        df = pd.read_excel(uploaded_file, engine='openpyxl')

        if df is None or df.empty:
            st.warning("O arquivo Excel enviado está vazio ou não pôde ser lido.")
            st.stop()

        df.columns = df.columns.str.strip().str.upper()

        if not {'ITENS', 'VENDA'}.issubset(df.columns):
            st.error("O relatório precisa ter as colunas 'ITENS' e 'VENDA'.")
            st.stop()

        df['ITENS_NORM'] = normalizar_serie(df['ITENS'])
        df['VENDA'] = pd.to_numeric(df['VENDA'], errors='coerce').fillna(0)
        df = df[(df['VENDA'] > 0) & (df['ITENS_NORM'].str.len() > 0)].copy()
        
        if df.empty:
            st.info('Nenhum dado válido (com vendas > 0) encontrado no relatório após a limpeza.')
            st.stop()

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

        # --- Exibição das Métricas ---
        st.subheader("Resumo da Análise")
        itens_total = df['ITENS'].nunique()
        itens_casados = df.dropna(subset=['FABRICANTE_FINAL'])['ITENS'].nunique()
        match_rate = (itens_casados / itens_total * 100) if itens_total else 0
        vendas_total = df['VENDA'].sum()
        vendas_casadas = df.dropna(subset=['FABRICANTE_FINAL'])['VENDA'].sum()
        vendas_match_rate = (vendas_casadas / vendas_total * 100) if vendas_total else 0

        col1, col2 = st.columns(2)
        col1.metric("Correspondência de Itens", f"{match_rate:.1f}%", f"{itens_casados} de {itens_total} produtos")
        col2.metric("Correspondência de Vendas", f"{vendas_match_rate:.1f}%", f"R$ {vendas_casadas:,.2f} de R$ {vendas_total:,.2f}")

        # --- Exibição dos Gráficos ---
        st.subheader("Visualizações")
        col1_graf, col2_graf = st.columns(2)

        with col1_graf:
            top_itens_df = df.groupby('ITENS')['VENDA'].sum().reset_index().nlargest(10, 'VENDA').sort_values('VENDA', ascending=True)
            fig_top_itens = px.bar(top_itens_df, x='VENDA', y='ITENS', orientation='h', title='Top 10 Itens Mais Vendidos', text_auto='.2s', color='VENDA', color_continuous_scale='Blues')
            st.plotly_chart(fig_top_itens, use_container_width=True)

        with col2_graf:
            df_fab = df.dropna(subset=['FABRICANTE_FINAL'])
            if df_fab.empty:
                st.info("Nenhum fabricante encontrado para exibir no gráfico.")
            else:
                fab_df = df_fab.groupby('FABRICANTE_FINAL')['VENDA'].sum().reset_index().nlargest(15, 'VENDA').sort_values('VENDA', ascending=False)
                fig_fabricantes = px.bar(fab_df, x='FABRICANTE_FINAL', y='VENDA', title='Top 15 Fabricantes por Venda', text_auto='.2s', color='VENDA', color_continuous_scale='Greens')
                fig_fabricantes.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig_fabricantes, use_container_width=True)
        
        # --- Exibição das Tabelas de Detalhes ---
        st.subheader("Detalhes da Correspondência")
        
        with st.expander("Tabela de Conferência (Top 50 Produtos Casados)"):
            tabela_conf_df = df[df['MATCH_TIPO'].notna()].groupby(['ITENS', 'PRODUTO_CATALOGO', 'FABRICANTE_FINAL', 'MATCH_TIPO', 'MATCH_SCORE'])['VENDA'].sum().reset_index().sort_values('VENDA', ascending=False).head(50)
            st.dataframe(tabela_conf_df)

        with st.expander("Relatório de Produtos Não Encontrados"):
            if produtos_nao_encontrados:
                vendas_nao_encontradas = df[df['ITENS_NORM'].isin(produtos_nao_encontrados)]['VENDA'].sum()
                st.warning(f"Total em vendas não casadas: R$ {vendas_nao_encontradas:,.2f}")
                nao_encontrados_df = df[df['ITENS_NORM'].isin(produtos_nao_encontrados)].groupby('ITENS')['VENDA'].sum().reset_index().sort_values('VENDA', ascending=False)
                st.dataframe(nao_encontrados_df)
            else:
                st.success("Todos os produtos foram encontrados!")

    except Exception as e:
        st.error(f"Ocorreu um erro ao processar o arquivo: {e}")
        logger.error(f"Erro no processamento do upload: {e}", exc_info=True)

else:
    st.info("Aguardando o upload de um relatório de vendas para iniciar a análise.")