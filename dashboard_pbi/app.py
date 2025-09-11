# app.py - Versão Melhorada com Match Fuzzy e Debug

import unicodedata
import pandas as pd
import plotly.express as px
from flask import Flask, request, render_template, flash
from fuzzywuzzy import fuzz, process
import os
import logging

app = Flask(__name__)
app.secret_key = 'sua_chave_secreta_aqui'

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Normalização de texto ---
def remover_acentos(s: str) -> str:
    """Remove acentos e caracteres especiais de uma string"""
    if not isinstance(s, str):
        s = str(s)
    nfkd = unicodedata.normalize('NFKD', s)
    return ''.join(ch for ch in nfkd if not unicodedata.combining(ch))

def normalizar_serie(serie: pd.Series) -> pd.Series:
    """Normaliza uma série pandas removendo acentos, espaços extras e padronizando"""
    return (
        serie.astype(str)
             .str.strip()
             .str.upper()
             .map(remover_acentos)
             .str.replace(r'\s+', ' ', regex=True)
    )

def encontrar_melhor_match(produto, catalogo_produtos, threshold=80):
    """Encontra o melhor match usando similaridade fuzzy"""
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
    """Cria mapeamento entre produtos do relatório e catálogo usando match fuzzy"""
    produtos_vendas = df_vendas['ITENS_NORM'].unique()
    produtos_catalogo = catalogo_df['DESCRICAO_NORM'].tolist()
    
    mapeamento = {}
    produtos_nao_encontrados = []
    
    logger.info(f"Iniciando match para {len(produtos_vendas)} produtos únicos...")
    
    for produto in produtos_vendas:
        if pd.isna(produto) or produto == '':
            continue
            
        # Primeiro tenta match exato
        match_exato = catalogo_df[catalogo_df['DESCRICAO_NORM'] == produto]
        
        if not match_exato.empty:
            fabricante = match_exato.iloc[0]['FABRICANTE']
            mapeamento[produto] = (produto, fabricante, 100, 'EXATO')
            logger.info(f"Match EXATO: '{produto}' -> Fabricante: {fabricante}")
        else:
            # Tenta match fuzzy
            match_result = encontrar_melhor_match(produto, produtos_catalogo, threshold)
            
            if match_result:
                produto_encontrado, score = match_result
                fabricante_row = catalogo_df[catalogo_df['DESCRICAO_NORM'] == produto_encontrado]
                
                if not fabricante_row.empty:
                    fabricante = fabricante_row.iloc[0]['FABRICANTE']
                    mapeamento[produto] = (produto_encontrado, fabricante, score, 'FUZZY')
                    logger.info(f"Match FUZZY ({score}%): '{produto}' -> '{produto_encontrado}' -> Fabricante: {fabricante}")
                else:
                    produtos_nao_encontrados.append(produto)
            else:
                produtos_nao_encontrados.append(produto)
                logger.warning(f"Produto NÃO encontrado: '{produto}'")
    
    logger.info(f"Match concluído: {len(mapeamento)} produtos mapeados, {len(produtos_nao_encontrados)} não encontrados")
    
    return mapeamento, produtos_nao_encontrados

# --- Carrega e prepara o catálogo uma vez ---
CATALOGO_PATH = 'catalogo_produtos.xlsx'

def carregar_catalogo():
    """Carrega e prepara o catálogo de produtos"""
    try:
        if not os.path.exists(CATALOGO_PATH):
            logger.error(f"Arquivo de catálogo não encontrado: {CATALOGO_PATH}")
            return None, f"Arquivo {CATALOGO_PATH} não encontrado"
        
        catalogo_df = pd.read_excel(CATALOGO_PATH, engine='openpyxl')
        catalogo_df.columns = catalogo_df.columns.str.strip().str.upper()
        
        # Verifica se as colunas necessárias existem
        colunas_necessarias = ['DESCRICAO', 'FABRICANTE']
        colunas_faltando = [col for col in colunas_necessarias if col not in catalogo_df.columns]
        
        if colunas_faltando:
            erro = f"Colunas faltando no catálogo: {colunas_faltando}. Colunas disponíveis: {list(catalogo_df.columns)}"
            logger.error(erro)
            return None, erro
        
        # Normaliza descrições
        catalogo_df['DESCRICAO_NORM'] = normalizar_serie(catalogo_df['DESCRICAO'])
        
        # Remove duplicatas
        catalogo_original = len(catalogo_df)
        catalogo_df = catalogo_df.drop_duplicates(subset=['DESCRICAO_NORM']).reset_index(drop=True)
        catalogo_final = len(catalogo_df)
        
        if catalogo_original != catalogo_final:
            logger.info(f"Removidas {catalogo_original - catalogo_final} duplicatas do catálogo")
        
        logger.info(f"Catálogo carregado com sucesso: {len(catalogo_df)} produtos únicos")
        return catalogo_df, None
        
    except Exception as e:
        erro = f"Erro ao carregar catálogo: {str(e)}"
        logger.error(erro)
        return None, erro

# Carrega o catálogo na inicialização
catalogo_df, catalogo_erro = carregar_catalogo()

@app.route('/', methods=['GET', 'POST'])
def upload_analise():
    """Rota principal para upload e análise do relatório"""
    
    # Informações de debug para o template
    debug_info = {
        'catalogo_status': 'OK' if catalogo_df is not None else 'ERRO',
        'catalogo_error': catalogo_erro or 'Nenhum',
        'catalogo_path_esperado': os.path.abspath(CATALOGO_PATH),
        'arquivos_no_diretorio': str(os.listdir('.'))
    }
    
    if catalogo_df is None:
        flash(f'Erro no catálogo: {catalogo_erro}')
        return render_template('upload.html', debug_info=debug_info)
    
    if request.method == 'POST':
        file = request.files.get('file')
        if not file or file.filename == '':
            flash('Nenhum arquivo selecionado.')
            return render_template('upload.html', debug_info=debug_info)

        try:
            # Lê relatório de vendas
            df = pd.read_excel(file, engine='openpyxl')
            df.columns = df.columns.str.strip().str.upper()

            # Validação das colunas necessárias
            if not {'ITENS', 'VENDA'}.issubset(df.columns):
                flash("O relatório precisa ter as colunas 'ITENS' e 'VENDA'.")
                return render_template('upload.html', debug_info=debug_info)

            # Normaliza e limpa dados
            df['ITENS_NORM'] = normalizar_serie(df['ITENS'])
            df['VENDA'] = pd.to_numeric(df['VENDA'], errors='coerce').fillna(0)
            
            # Remove linhas com vendas zero ou itens vazios
            df = df[(df['VENDA'] > 0) & (df['ITENS_NORM'].str.len() > 0)]
            
            if df.empty:
                flash('Nenhum dado válido encontrado no relatório.')
                return render_template('upload.html', debug_info=debug_info)

            logger.info(f"Processando relatório com {len(df)} linhas válidas")

            # Cria mapeamento usando match fuzzy
            threshold = int(request.form.get('threshold', 80))
            mapeamento, produtos_nao_encontrados = criar_mapeamento_produtos(df, catalogo_df, threshold)

            # Aplica o mapeamento
            df['FABRICANTE_FINAL'] = None
            df['PRODUTO_CATALOGO'] = None
            df['MATCH_SCORE'] = None
            df['MATCH_TIPO'] = None

            for produto_venda in df['ITENS_NORM'].unique():
                if produto_venda in mapeamento:
                    produto_cat, fabricante, score, tipo = mapeamento[produto_venda]
                    mask = df['ITENS_NORM'] == produto_venda
                    df.loc[mask, 'FABRICANTE_FINAL'] = fabricante
                    df.loc[mask, 'PRODUTO_CATALOGO'] = produto_cat
                    df.loc[mask, 'MATCH_SCORE'] = score
                    df.loc[mask, 'MATCH_TIPO'] = tipo

            # --------- Gráfico 1: Top 10 Itens ----------
            top_itens_df = (
                df.groupby('ITENS', as_index=False)['VENDA']
                  .sum()
                  .nlargest(10, 'VENDA')
                  .sort_values('VENDA', ascending=True)
            )
            
            fig_top_itens = px.bar(
                top_itens_df, 
                x='VENDA', 
                y='ITENS',
                orientation='h',
                title='Top 10 Itens Mais Vendidos',
                text_auto='.2s',
                color='VENDA',
                color_continuous_scale='Blues'
            )
            fig_top_itens.update_layout(height=500)

            # --------- Gráfico 2: Top 15 Fabricantes ----------
            df_fab = df.dropna(subset=['FABRICANTE_FINAL'])
            
            if df_fab.empty:
                grafico_fabricantes_html = "<div class='alert alert-warning'>Nenhum fabricante encontrado no match.</div>"
            else:
                fab_df = (
                    df_fab.groupby('FABRICANTE_FINAL', as_index=False)['VENDA']
                          .sum()
                          .nlargest(15, 'VENDA')
                          .sort_values('VENDA', ascending=False)
                )
                
                fig_fabricantes = px.bar(
                    fab_df, 
                    x='FABRICANTE_FINAL', 
                    y='VENDA',
                    title='Top 15 Fabricantes por Venda',
                    text_auto='.2s',
                    color='VENDA',
                    color_continuous_scale='Greens'
                )
                fig_fabricantes.update_layout(
                    height=500,
                    xaxis_tickangle=-45
                )
                grafico_fabricantes_html = fig_fabricantes.to_html(full_html=False)

            # --------- Tabela de Conferência ----------
            tabela_conf_df = (
                df.groupby(['ITENS', 'FABRICANTE_FINAL', 'MATCH_TIPO', 'MATCH_SCORE'], as_index=False)['VENDA']
                  .sum()
                  .sort_values('VENDA', ascending=False)
                  .head(50)
            )
            
            # Formata a tabela
            tabela_conf_df['VENDA_FORMATADA'] = tabela_conf_df['VENDA'].apply(lambda x: f'R$ {x:,.2f}')
            tabela_conf_df['MATCH_SCORE'] = tabela_conf_df['MATCH_SCORE'].fillna(0).astype(int)
            
            tabela_conf_html = tabela_conf_df[['ITENS', 'FABRICANTE_FINAL', 'MATCH_TIPO', 'MATCH_SCORE', 'VENDA_FORMATADA']].to_html(
                index=False, 
                classes="table table-striped table-sm table-hover",
                table_id="tabela-conferencia"
            )

            # --------- Relatório de Produtos Não Encontrados ----------
            if produtos_nao_encontrados:
                vendas_nao_encontradas = df[df['ITENS_NORM'].isin(produtos_nao_encontrados)]['VENDA'].sum()
                nao_encontrados_df = (
                    df[df['ITENS_NORM'].isin(produtos_nao_encontrados)]
                    .groupby('ITENS', as_index=False)['VENDA']
                    .sum()
                    .sort_values('VENDA', ascending=False)
                )
                nao_encontrados_df['VENDA_FORMATADA'] = nao_encontrados_df['VENDA'].apply(lambda x: f'R$ {x:,.2f}')
                
                nao_encontrados_html = nao_encontrados_df[['ITENS', 'VENDA_FORMATADA']].to_html(
                    index=False,
                    classes="table table-striped table-sm table-danger"
                )
            else:
                vendas_nao_encontradas = 0
                nao_encontrados_html = "<div class='alert alert-success'>Todos os produtos foram encontrados!</div>"

            # --------- Métricas de Match ----------
            itens_total = df['ITENS'].nunique()
            itens_casados = df.dropna(subset=['FABRICANTE_FINAL'])['ITENS'].nunique()
            match_rate = (itens_casados / itens_total * 100) if itens_total else 0
            
            vendas_total = df['VENDA'].sum()
            vendas_casadas = df.dropna(subset=['FABRICANTE_FINAL'])['VENDA'].sum()
            vendas_match_rate = (vendas_casadas / vendas_total * 100) if vendas_total else 0
            
            # Estatísticas por tipo de match
            stats_match = df.dropna(subset=['MATCH_TIPO']).groupby('MATCH_TIPO').agg({
                'ITENS': 'nunique',
                'VENDA': 'sum'
            }).reset_index()

            # Renderiza o template com todos os dados
            return render_template('dashboard.html',
                                 grafico_top_itens=fig_top_itens.to_html(full_html=False),
                                 grafico_fabricantes=grafico_fabricantes_html,
                                 tabela_conferencia=tabela_conf_html,
                                 produtos_nao_encontrados=nao_encontrados_html,
                                 itens_total=itens_total,
                                 itens_casados=itens_casados,
                                 match_rate=round(match_rate, 1),
                                 vendas_total=f'R