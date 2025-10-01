import os
import psycopg2
import pandas as pd
from flask import Flask, jsonify, render_template, request
import csv
import io
from psycopg2.extras import execute_values

# --- INICIALIZAÇÃO EXPLÍCITA DO FLASK ---
app = Flask(__name__, template_folder='templates', static_folder='../static')

# =================================================================
# 1. FUNÇÕES DE INICIALIZAÇÃO E CONEXÃO COM A BASE DE DADOS
# =================================================================

def init_db():
    """Verifica e cria as tabelas da base de dados, se não existirem."""
    conn = None
    try:
        conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
        with conn.cursor() as cur:
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
# 2. ROTAS DE PÁGINA E API
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
    """Recebe o ficheiro de vendas e processa-o diretamente."""
    if 'salesFile' not in request.files:
        return jsonify({"message": "Nenhum ficheiro de vendas enviado"}), 400
    
    file = request.files['salesFile']
    if file.filename == '':
        return jsonify({"message": "Nenhum ficheiro selecionado"}), 400

    app.logger.info(f"A processar o upload do ficheiro de vendas: {file.filename}")
    conn = None
    try:
        df = None
        file.seek(0)
        filename = file.filename.lower()

        if filename.endswith('.xlsx'):
            try:
                df = pd.read_excel(file, engine='openpyxl', skiprows=8)
            except Exception:
                file.seek(0)
                df = pd.read_excel(file, engine='openpyxl')
        elif filename.endswith('.csv'):
            for encoding in ['utf-8', 'latin-1', 'iso-8859-1']:
                try:
                    file.seek(0)
                    df = pd.read_csv(file, sep='[;,]', engine='python', on_bad_lines='skip', encoding=encoding)
                    if df.shape[1] > 3: break
                    else: df = None
                except Exception: continue
        
        if df is None:
            raise ValueError("O formato do ficheiro de vendas não é reconhecido.")

        column_map = {'data faturamento': 'data_venda', 'data': 'data_venda', 'vendedor': 'vendedor', 'fabricante': 'fabricante', 'nome fantasia': 'cliente', 'cliente': 'cliente', 'descricao produto': 'produto', 'quantidade vendida': 'quantidade', 'valor total item': 'valor', 'valor total': 'valor', 'valor': 'valor'}
        df.rename(columns=lambda c: str(c).strip().lower(), inplace=True)
        df.rename(columns=column_map, inplace=True)
        
        required_cols = ['data_venda', 'vendedor', 'fabricante', 'cliente', 'produto', 'quantidade', 'valor']
        
        if not all(col in df.columns for col in required_cols):
             missing = [col for col in required_cols if col not in df.columns]
             return jsonify({"message": f"Colunas essenciais em falta no ficheiro de vendas: {', '.join(missing)}"}), 400

        df = df[required_cols]
        df['data_venda'] = pd.to_datetime(df['data_venda'], errors='coerce')
        df.dropna(subset=['data_venda'], inplace=True)
        # Converte colunas numéricas, forçando erros para NaN, depois preenche NaN com 0
        for col in ['quantidade', 'valor']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        conn = get_db_connection()
        with conn.cursor() as cur:
            if not df.empty:
                first_date_str = df['data_venda'].dropna().astype(str).iloc[0]
                first_date = pd.to_datetime(first_date_str).strftime('%Y-%m-01')
                cur.execute("DELETE FROM public.vendas WHERE data_venda >= %s AND data_venda < CAST(%s AS DATE) + INTERVAL '1 month'", (first_date, first_date))
                
                data_tuples = [tuple(x) for x in df.to_numpy()]
                
                execute_values(cur, 
                    "INSERT INTO public.vendas (data_venda, vendedor, fabricante, cliente, produto, quantidade, valor) VALUES %s",
                    data_tuples)
        conn.commit()
        
        return jsonify({"message": "Ficheiro de vendas processado com sucesso."}), 200

    except Exception as e:
        app.logger.error(f"Erro GERAL na função upload_sales: {e}", exc_info=True)
        return jsonify({"message": f"Erro ao processar o ficheiro: {str(e)}"}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/upload/carteira', methods=['POST'])
def upload_portfolio():
    """Recebe o ficheiro da carteira e processa-o diretamente."""
    if 'portfolioFile' not in request.files:
        return jsonify({"message": "Nenhum ficheiro de carteira enviado"}), 400
    
    file = request.files['portfolioFile']
    if file.filename == '':
        return jsonify({"message": "Nenhum ficheiro selecionado"}), 400
    
    conn = None
    try:
        df = None
        file.seek(0)
        
        try:
            df = pd.read_excel(file, engine='openpyxl')
        except Exception:
            file.seek(0)
            try:
                content = file.read().decode('utf-8')
                file.seek(0)
                if '"' in content.splitlines()[0] and ';' in content.splitlines()[0]:
                     reader = csv.reader(io.StringIO(content), delimiter=';', quotechar='"')
                     rows = list(reader)
                     header = [h.strip() for h in (rows[0][0].split(';') + rows[0][1:])]
                     fixed_data = [(r[0].split(';') + r[1:]) for r in rows[1:]]
                     df = pd.DataFrame(fixed_data, columns=header)
                else:
                    df = pd.read_csv(file, sep='[;,]', engine='python', on_bad_lines='skip')
            except Exception:
                 raise ValueError("Não foi possível ler o ficheiro da carteira. Verifique o formato.")

        if df is None:
            raise ValueError("O formato do ficheiro da carteira não é reconhecido.")

        column_map = {'vendedor': 'vendedor', 'total clientes': 'total_clientes', 'total clentes': 'total_clientes', 'total produtos': 'total_produtos', 'meta faturamento': 'meta_faturamento'}
        df.rename(columns=lambda c: str(c).strip().lower(), inplace=True)
        df.rename(columns=column_map, inplace=True)

        required_cols = ['vendedor', 'total_clientes', 'total_produtos']
        if not all(col in df.columns for col in required_cols):
            missing = [col for col in required_cols if col not in df.columns]
            return jsonify({"message": f"Colunas essenciais em falta no ficheiro de carteira: {', '.join(missing)}"}), 400
        
        if 'meta_faturamento' not in df.columns:
            df['meta_faturamento'] = 0

        df = df[required_cols + ['meta_faturamento']]
        df.dropna(subset=required_cols, inplace=True)
        
        df['total_clientes'] = pd.to_numeric(df['total_clientes'], errors='coerce').fillna(0).astype(int)
        df['total_produtos'] = pd.to_numeric(df['total_produtos'], errors='coerce').fillna(0).astype(int)
        df['meta_faturamento'] = pd.to_numeric(df['meta_faturamento'].astype(str).str.replace(r'[R$.]', '', regex=True).str.replace(',', '.'), errors='coerce').fillna(0)

        conn = get_db_connection()
        with conn.cursor() as cur:
            data_tuples = [tuple(x) for x in df.to_numpy()]
            
            execute_values(cur, 
                """
                INSERT INTO public.carteira (vendedor, total_clientes, total_produtos, meta_faturamento) 
                VALUES %s
                ON CONFLICT (vendedor) 
                DO UPDATE SET 
                    total_clientes = EXCLUDED.total_clientes, 
                    total_produtos = EXCLUDED.total_produtos,
                    meta_faturamento = EXCLUDED.meta_faturamento;
                """,
                data_tuples)
        conn.commit()
        
        return jsonify({"message": "Ficheiro de carteira processado com sucesso."}), 200

    except Exception as e:
        app.logger.error(f"Erro ao processar ficheiro de carteira: {e}", exc_info=True)
        return jsonify({"message": f"Erro ao processar o ficheiro: {str(e)}"}), 500
    finally:
        if conn:
            conn.close()


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
        if conn:
            conn.close()

# --- INICIALIZA A BASE DE DADOS NA ARRANCADA DA APLICAÇÃO ---
init_db()

