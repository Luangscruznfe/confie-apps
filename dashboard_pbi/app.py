# app.py - Versão ajustada para achar catalogo relativo ao app.py e logs melhores

import unicodedata
import pandas as pd
import plotly.express as px
from flask import Flask, request, render_template, flash
from fuzzywuzzy import fuzz, process
import os
import logging
import glob

# Configuração básica
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CATALOGO_PATH = os.path.join(BASE_DIR, 'catalogo_produtos.xlsx')

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'chave_de_dev_temporaria')

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Funções de Normalização e Match ---
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
        if not match:
            return None
        # match pode ter 2 ou 3 elementos; padronizamos para (texto, score)
        match_text = match[0]
        match_score = match[1] if len(match) > 1 else 0
        if match_score >= threshold:
            return (match_text, match_score)
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

# --- Carrega e prepara o catálogo uma vez ---
def carregar_catalogo():
    try:
        logger.info(f"BASE_DIR: {BASE_DIR}")
        logger.info(f"Procurando catálogo em: {CATALOGO_PATH}")
        logger.info(f"Arquivos em BASE_DIR: {os.listdir(BASE_DIR)}")

        # Tenta caminho direto primeiro
        if os.path.exists(CATALOGO_PATH):
            catalogo_df = pd.read_excel(CATALOGO_PATH, engine='openpyxl')
            logger.info(f"Catálogo carregado diretamente de {CATALOGO_PATH}")
        else:
            # Fallback: procura recursivamente por arquivos com 'catalogo' no nome
            padrao = os.path.join(BASE_DIR, '**', '*catalogo*.xlsx')
            encontrados = glob.glob(padrao, recursive=True)
            if encontrados:
                encontrado = encontrados[0]
                logger.warning(f"Arquivo de catálogo encontrado em local alternativo: {encontrado}")
                catalogo_df = pd.read_excel(encontrado, engine='openpyxl')
            else:
                erro = f"Arquivo {CATALOGO_PATH} não encontrado em BASE_DIR e subpastas."
                logger.error(erro)
                return None, erro

        catalogo_df.columns = catalogo_df.columns.str.strip().str.upper()
        
        colunas_necessarias = ['DESCRICAO', 'FABRICANTE']
        colunas_faltando = [col for col in colunas_necessarias if col not in catalogo_df.columns]
        
        if colunas_faltando:
            erro = f"Colunas faltando no catálogo: {colunas_faltando}"
            logger.error(erro)
            return None, erro
        
        catalogo_df['DESCRICAO_NORM'] = normalizar_serie(catalogo_df['DESCRICAO'])
        catalogo_df = catalogo_df.drop_duplicates(subset=['DESCRICAO_NORM']).reset_index(drop=True)
        
        logger.info(f"Catálogo carregado com sucesso: {len(catalogo_df)} produtos únicos")
        return catalogo_df, None
        
    except Exception as e:
        erro = f"Erro ao carregar catálogo: {str(e)}"
        logger.error(erro, exc_info=True)
        return None, erro

catalogo_df, catalogo_erro = carregar_catalogo()

@app.route('/', methods=['GET', 'POST'])
def upload_analise():
    debug_info = {
        'catalogo_status': 'OK' if catalogo_df is not None else 'ERRO',
        'catalogo_error': catalogo_erro or 'Nenhum',
        'catalogo_path_esperado': CATALOGO_PATH,
        'arquivos_no_diretorio': str(os.listdir(BASE_DIR))
    }
    
    if catalogo_df is None:
        flash(f'Erro Crítico no Catálogo: {catalogo_erro}. A aplicação não pode continuar.')
        return render_template('upload.html', debug_info=debug_info)
    
    if request.method == 'POST':
        file = request.files.get('file')
        if not file or file.filename == '':
            flash('Nenhum arquivo selecionado.')
            return render_template('upload.html', debug_info=debug_info)

        try:
            df = pd.read_excel(file, engine='openpyxl')
            
            if df is None or df.empty:
                flash("Erro: O arquivo Excel não pôde ser lido, está vazio ou corrompido.")
                return render_template('upload.html', debug_info=debug_info)

            df.columns = df.columns.str.strip().str.upper()

            if not {'ITENS', 'VENDA'}.issubset(df.columns):
                flash("O relatório precisa ter as colunas 'ITENS' e 'VENDA'.")
                return render_template('upload.html', debug_info=debug_info)

            df['ITENS_NORM'] = normalizar_serie(df['ITENS'])
            df['VENDA'] = pd.to_numeric(df['VENDA'], errors='coerce').fillna(0)
            df = df[(df['VENDA'] > 0) & (df['ITENS_NORM'].str.len() > 0)].copy()
            
            if df.empty:
                flash('Nenhum dado válido encontrado no relatório após a limpeza.')
                return render_template('upload.html', debug_info=debug_info)

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

            # --------- Gráfico 1: Top 10 Itens (SINTAXE CORRIGIDA) ----------
            top_itens_df = (
                df.groupby('ITENS')['VENDA']
                  .sum()
                  .reset_index()
                  .nlargest(10, 'VENDA')
                  .sort_values('VENDA', ascending=True)
            )
            
            fig_top_itens = px.bar(
                top_itens_df, 
                x='VENDA', 
                y='ITENS',
                orientation='h',
                title='Top 10 Itens Mais Vendidos',
                text_auto=True,
                color='VENDA',
                color_continuous_scale='Blues'
            )
            fig_top_itens.update_layout(height=500)

            # --------- Gráfico 2: Top 15 Fabricantes (SINTAXE CORRIGIDA) ----------
            df_fab = df.dropna(subset=['FABRICANTE_FINAL'])
            
            if df_fab.empty:
                grafico_fabricantes_html = "<div class='alert alert-warning'>Nenhum fabricante encontrado.</div>"
            else:
                fab_df = (
                    df_fab.groupby('FABRICANTE_FINAL')['VENDA']
                          .sum()
                          .reset_index()
                          .nlargest(15, 'VENDA')
                          .sort_values('VENDA', ascending=False)
                )
                
                fig_fabricantes = px.bar(
                    fab_df, 
                    x='FABRICANTE_FINAL', 
                    y='VENDA',
                    title='Top 15 Fabricantes por Venda',
                    text_auto=True,
                    color='VENDA',
                    color_continuous_scale='Greens'
                )
                fig_fabricantes.update_layout(height=500, xaxis_tickangle=-45)
                grafico_fabricantes_html = fig_fabricantes.to_html(full_html=False)

            # --------- Tabela de Conferência (SINTAXE CORRIGIDA) ----------
            tabela_conf_df = (
                df.groupby(['ITENS', 'FABRICANTE_FINAL', 'MATCH_TIPO', 'MATCH_SCORE'])['VENDA']
                  .sum()
                  .reset_index()
                  .sort_values('VENDA', ascending=False)
                  .head(50)
            )
            
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
                    .groupby('ITENS')['VENDA']
                    .sum()
                    .reset_index()
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

            # Renderiza o template com todos os dados
            return render_template('dashboard.html',
                                   grafico_top_itens=fig_top_itens.to_html(full_html=False),
                                   grafico_fabricantes=grafico_fabricantes_html,
                                   tabela_conferencia=tabela_conf_html,
                                   produtos_nao_encontrados=nao_encontrados_html,
                                   itens_total=itens_total,
                                   itens_casados=itens_casados,
                                   match_rate=round(match_rate, 1),
                                   vendas_total=f'R$ {vendas_total:,.2f}',
                                   vendas_casadas=f'R$ {vendas_casadas:,.2f}',
                                   vendas_match_rate=round(vendas_match_rate, 1),
                                   vendas_nao_encontradas=f'R$ {vendas_nao_encontradas:,.2f}',
                                   threshold_usado=threshold)

        except Exception as e:
            flash(f'Erro ao processar arquivo: {str(e)}')
            logger.error(f"Erro no processamento: {e}", exc_info=True)
            return render_template('upload.html', debug_info=debug_info)
    
    return render_template('upload.html', debug_info=debug_info)

if __name__ == '__main__':
    app.run(debug=True)
