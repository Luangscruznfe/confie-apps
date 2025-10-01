import os
import psycopg2
import pandas as pd
import threading
from flask import Flask, jsonify, render_template, request

# --- INICIALIZAÇÃO EXPLÍCITA DO FLASK ---
app = Flask(__name__, template_folder='templates')

# =================================================================
# 1. FUNÇÕES DE INICIALIZAÇÃO E CONEXÃO COM A BASE DE DADOS
# =================================================================

def init_db():
    """Verifica e cria as tabelas da base de dados, se não existirem."""
    conn = None
    try:
        conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
        with conn.cursor() as cur:
            # Cria a tabela de vendas se ela não existir
            cur.execute("""
                CREATE TABLE IF NOT EXISTS public.vendas (
                    id SERIAL PRIMARY KEY,
                    data_venda DATE NOT NULL,
                    vendedor VARCHAR(255),
                    fabricante VARCHAR(255),
                    cliente VARCHAR(255),
                    produto VARCHAR(255),
                    quantidade INTEGER,
                    valor NUMERIC(10, 2)
                );
            """)
            # Cria a tabela de carteira se ela não existir
            cur.execute("""
                CREATE TABLE IF NOT EXISTS public.carteira (
                    vendedor VARCHAR(255) PRIMARY KEY,
                    total_clientes INTEGER,
                    total_produtos INTEGER,
                    meta_faturamento NUMERIC(12, 2)
                );
            """)
        conn.commit()
        app.logger.info("Base de dados inicializada com sucesso.")
    except Exception as e:
        app.logger.error(f"Erro ao inicializar a base de dados: {e}")
    finally:
        if conn:
            conn.close()

def get_db_connection():
    """Cria e retorna uma nova conexão com a base de dados."""
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
    return conn

# =================================================================
# 2. FUNÇÕES DE PROCESSAMENTO EM SEGUNDO PLANO
# =================================================================

def process_sales_in_background(df):
    """Lê o DataFrame de vendas e insere na base de dados."""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            first_date = pd.to_datetime(df['data_venda'].iloc[0]).strftime('%Y-%m-01')
            cur.execute("DELETE FROM public.vendas WHERE data_venda >= %s AND data_venda < CAST(%s AS DATE) + INTERVAL '1 month'", (first_date, first_date))

            for index, row in df.iterrows():
                cur.execute(
                    "INSERT INTO public.vendas (data_venda, vendedor, fabricante, cliente, produto, quantidade, valor) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (row['data_venda'], row['vendedor'], row['fabricante'], row['cliente'], row['produto'], row['quantidade'], row['valor'])
                )
        conn.commit()
    except Exception as e:
        app.logger.error(f"Erro no processamento em segundo plano (vendas): {e}")
    finally:
        if conn:
            conn.close()
    app.logger.info("Processamento em segundo plano (vendas) concluído.")

def process_portfolio_in_background(df):
    """Lê o DataFrame da carteira e insere/atualiza na base de dados."""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            for index, row in df.iterrows():
                cur.execute(
                    """
                    INSERT INTO public.carteira (vendedor, total_clientes, total_produtos, meta_faturamento) 
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (vendedor) 
                    DO UPDATE SET 
                        total_clientes = EXCLUDED.total_clientes, 
                        total_produtos = EXCLUDED.total_produtos,
                        meta_faturamento = EXCLUDED.meta_faturamento;
                    """,
                    (row['vendedor'], row['total_clientes'], row['total_produtos'], row['meta_faturamento'])
                )
        conn.commit()
    except Exception as e:
        app.logger.error(f"Erro no processamento em segundo plano (carteira): {e}")
    finally:
        if conn:
            conn.close()
    app.logger.info("Processamento em segundo plano (carteira) concluído.")

# =================================================================
# 3. ROTAS DE PÁGINA E API
# =================================================================

@app.route("/")
def index():
    """Serve a página principal do dashboard."""
    return render_template('dashboard.html')

@app.route("/api/dados", methods=['GET'])
def get_data():
    """Busca todos os dados de vendas e carteira da base de dados."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT TO_CHAR(data_venda, 'YYYY-MM-DD') as \"Data\", vendedor as \"Vendedor\", fabricante as \"Fabricante\", cliente as \"Cliente\", produto as \"Produto\", quantidade as \"Quantidade\", valor as \"Valor\" FROM public.vendas")
            sales_data = cur.fetchall()
            sales_columns = [desc[0] for desc in cur.description]
            sales_list = [dict(zip(sales_columns, row)) for row in sales_data]

            cur.execute("SELECT vendedor, total_clientes, total_produtos, meta_faturamento FROM public.carteira")
            portfolio_data = cur.fetchall()
            portfolio_list = [[row[0], {"totalClientes": row[1], "totalProdutos": row[2], "meta": float(row[3]) if row[3] is not None else 0}] for row in portfolio_data]

            if not sales_list and not portfolio_list:
                return jsonify({"message": "Nenhum dado encontrado"}), 404

            return jsonify({
                "salesData": sales_list,
                "portfolioData": portfolio_list
            })
    finally:
        conn.close()


@app.route('/api/upload/vendas', methods=['POST'])
def upload_sales():
    """Recebe o ficheiro de vendas e inicia o processamento em segundo plano."""
    if 'salesFile' not in request.files:
        return jsonify({"message": "Nenhum ficheiro de vendas enviado"}), 400
    
    file = request.files['salesFile']
    if file.filename == '':
        return jsonify({"message": "Nenhum ficheiro selecionado"}), 400

    try:
        df = pd.read_excel(file, engine='openpyxl', skiprows=8)

        # CORREÇÃO: Lógica de renomeação mais robusta e insensível a maiúsculas/minúsculas
        column_map = {
            'data faturamento': 'data_venda', 'data': 'data_venda',
            'vendedor': 'vendedor',
            'fabricante': 'fabricante',
            'nome fantasia': 'cliente', 'cliente': 'cliente',
            'descricao produto': 'produto',
            'quantidade vendida': 'quantidade',
            'valor total item': 'valor', 'valor total': 'valor', 'valor': 'valor'
        }
        
        df.rename(columns=lambda c: c.strip().lower(), inplace=True)
        df.rename(columns=column_map, inplace=True)
        
        required_cols = ['data_venda', 'vendedor', 'fabricante', 'cliente', 'produto', 'quantidade', 'valor']
        
        if not all(col in df.columns for col in required_cols):
             missing = [col for col in required_cols if col not in df.columns]
             return jsonify({"message": f"Colunas essenciais em falta no ficheiro de vendas: {', '.join(missing)}"}), 400

        df = df[required_cols]
        df.dropna(subset=required_cols, inplace=True)
        
        # Inicia a thread para processamento em segundo plano
        thread = threading.Thread(target=process_sales_in_background, args=(df,))
        thread.start()
        
        return jsonify({"message": "Ficheiro de vendas recebido. O processamento foi iniciado em segundo plano."}), 202

    except Exception as e:
        app.logger.error(f"Erro ao processar ficheiro de vendas: {e}")
        return jsonify({"message": f"Erro ao iniciar o processamento do ficheiro: {str(e)}"}), 500


@app.route('/api/upload/carteira', methods=['POST'])
def upload_portfolio():
    """Recebe o ficheiro da carteira e inicia o processamento em segundo plano."""
    if 'portfolioFile' not in request.files:
        return jsonify({"message": "Nenhum ficheiro de carteira enviado"}), 400
    
    file = request.files['portfolioFile']
    if file.filename == '':
        return jsonify({"message": "Nenhum ficheiro selecionado"}), 400

    try:
        df = pd.read_excel(file, engine='openpyxl')

        # CORREÇÃO: Lógica de renomeação mais robusta e insensível a maiúsculas/minúsculas
        column_map = {
            'vendedor': 'vendedor',
            'total clientes': 'total_clientes',
            'total produtos': 'total_produtos',
            'meta faturamento': 'meta_faturamento'
        }
        df.rename(columns=lambda c: c.strip().lower(), inplace=True)
        df.rename(columns=column_map, inplace=True)

        required_cols = ['vendedor', 'total_clientes', 'total_produtos']
        if not all(col in df.columns for col in required_cols):
            missing = [col for col in required_cols if col not in df.columns]
            return jsonify({"message": f"Colunas essenciais em falta no ficheiro de carteira: {', '.join(missing)}"}), 400
        
        if 'meta_faturamento' not in df.columns:
            df['meta_faturamento'] = 0

        df = df[required_cols + ['meta_faturamento']]
        df.dropna(subset=required_cols, inplace=True)
        df['meta_faturamento'] = pd.to_numeric(df['meta_faturamento'].astype(str).str.replace(r'[R$.]', '', regex=True).str.replace(',', '.'), errors='coerce').fillna(0)

        # Inicia a thread para processamento em segundo plano
        thread = threading.Thread(target=process_portfolio_in_background, args=(df,))
        thread.start()

        return jsonify({"message": "Ficheiro de carteira recebido. O processamento foi iniciado em segundo plano."}), 202

    except Exception as e:
        app.logger.error(f"Erro ao processar ficheiro de carteira: {e}")
        return jsonify({"message": f"Erro ao iniciar o processamento do ficheiro: {str(e)}"}), 500


@app.route("/api/dados", methods=['DELETE'])
def delete_data():
    """Apaga todos os dados das tabelas de vendas e carteira."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE public.vendas, public.carteira RESTART IDENTITY;")
        conn.commit()
        return jsonify({"message": "Todos os dados foram apagados com sucesso."}), 200
    except Exception as e:
        return jsonify({"message": f"Erro ao apagar dados: {str(e)}"}), 500
    finally:
        conn.close()

# --- INICIALIZA A BASE DE DADOS NA ARRANCADA DA APLICAÇÃO ---
init_db()

