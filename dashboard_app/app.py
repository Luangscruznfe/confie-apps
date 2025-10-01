import os
import pandas as pd
from flask import Flask, jsonify, request, render_template
import psycopg2
import psycopg2.extras

# O static_folder aponta para a pasta onde o dashboard.html está
app = Flask(__name__, static_folder='../static', template_folder='../static')

#=================================
# FUNÇÕES DE BANCO DE DADOS
#=================================
def get_db_connection():
    """Cria e retorna uma conexão com o banco de dados Neon."""
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
    return conn

#=================================
# ROTAS DA APLICAÇÃO
#=================================
@app.route('/', methods=['GET'])
def index():
    """Serve o arquivo principal do dashboard (dashboard.html)."""
    # Garante que o Flask procure o arquivo na pasta 'static'
    return render_template('dashboard.html')

#=================================
# ROTAS DE API (ENDPOINTS)
#=================================

@app.route('/api/dados', methods=['GET'])
def get_data():
    """Busca todos os dados de vendas e carteira do banco de dados."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Buscar dados de vendas
        cur.execute("SELECT TO_CHAR(data_venda, 'YYYY-MM-DD') as \"Data\", vendedor as \"Vendedor\", fabricante as \"Fabricante\", cliente as \"Cliente\", produto as \"Produto\", quantidade as \"Quantidade\", valor as \"Valor\" FROM vendas ORDER BY data_venda ASC;")
        sales_data = cur.fetchall()

        # Buscar dados da carteira
        cur.execute("SELECT vendedor, total_clientes, total_produtos, meta_faturamento FROM carteira;")
        portfolio_rows = cur.fetchall()
        
        # Formata para o padrão que o frontend espera: [ [vendedor, {dados}], ... ]
        portfolio_data = [[row['vendedor'], {'totalClientes': row['total_clientes'], 'totalProdutos': row['total_produtos'], 'meta': float(row['meta_faturamento'])}] for row in portfolio_rows]

        cur.close()

        if not sales_data or not portfolio_data:
            return jsonify({"message": "Nenhum dado encontrado"}), 404

        return jsonify({ "salesData": sales_data, "portfolioData": portfolio_data })

    except Exception as e:
        print(f"Erro ao buscar dados: {e}")
        return jsonify({"message": "Erro interno do servidor ao buscar dados"}), 500
    finally:
        if conn: conn.close()

@app.route('/api/upload/vendas', methods=['POST'])
def upload_vendas():
    """Recebe o arquivo de vendas, processa e salva no banco."""
    if 'salesFile' not in request.files:
        return jsonify({"message": "Nenhum arquivo de vendas enviado"}), 400

    file = request.files['salesFile']
    if file.filename == '':
        return jsonify({"message": "Nenhum arquivo selecionado"}), 400

    conn = None
    try:
        df = pd.read_excel(file, skiprows=8)

        # Mapeia os nomes das colunas do Excel para os nomes do banco
        column_map = {
            'Data Faturamento': 'data_venda', 'Vendedor': 'vendedor',
            'Fabricante': 'fabricante', 'Nome Fantasia': 'cliente',
            'Descricao Produto': 'produto', 'Quantidade Vendida': 'quantidade',
            'Valor Total Item': 'valor'
        }
        df.rename(columns=column_map, inplace=True)
        
        # Filtra apenas as colunas necessárias
        df = df[list(column_map.values())]

        df.dropna(subset=['data_venda'], inplace=True)
        df['data_venda'] = pd.to_datetime(df['data_venda']).dt.date
        df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
        df['quantidade'] = pd.to_numeric(df['quantidade'], errors='coerce')
        df.dropna(subset=['valor', 'quantidade'], inplace=True)

        if not df.empty:
            primeiro_dia_mes = df['data_venda'].min().replace(day=1)
            ultimo_dia_mes = pd.Period(primeiro_dia_mes, freq='M').end_time.date()

            conn = get_db_connection()
            cur = conn.cursor()
            
            # Deleta os dados do mês que está sendo importado para evitar duplicatas
            cur.execute("DELETE FROM vendas WHERE data_venda BETWEEN %s AND %s;", (primeiro_dia_mes, ultimo_dia_mes))

            data_tuples = [tuple(x) for x in df.to_numpy()]
            psycopg2.extras.execute_values(cur, "INSERT INTO vendas (data_venda, vendedor, fabricante, cliente, produto, quantidade, valor) VALUES %s", data_tuples)
            
            conn.commit()
            cur.close()

        return jsonify({"message": "Arquivo de vendas processado com sucesso!"}), 200

    except Exception as e:
        print(f"Erro ao processar arquivo de vendas: {e}")
        return jsonify({"message": f"Erro ao processar arquivo: {e}"}), 500
    finally:
        if conn: conn.close()

@app.route('/api/upload/carteira', methods=['POST'])
def upload_carteira():
    """Recebe o arquivo da carteira, limpa a tabela antiga e insere os novos dados."""
    if 'portfolioFile' not in request.files:
        return jsonify({"message": "Nenhum arquivo de carteira enviado"}), 400
    
    file = request.files['portfolioFile']
    if file.filename == '':
        return jsonify({"message": "Nenhum arquivo selecionado"}), 400

    conn = None
    try:
        df = pd.read_excel(file)
        
        column_map = {
            'Vendedor': 'vendedor', 'Total Clientes': 'total_clientes',
            'Total Produtos': 'total_produtos', 'Meta faturamento': 'meta_faturamento'
        }
        df.rename(columns=column_map, inplace=True)
        df = df[list(column_map.values())]

        df['meta_faturamento'] = df['meta_faturamento'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
        df['meta_faturamento'] = pd.to_numeric(df['meta_faturamento'], errors='coerce').fillna(0)
        df.dropna(inplace=True)

        conn = get_db_connection()
        cur = conn.cursor()

        # Limpa a tabela antes de inserir, pois a carteira é sempre um espelho do último arquivo
        cur.execute("TRUNCATE TABLE carteira;")
        
        data_tuples = [tuple(x) for x in df.to_numpy()]
        psycopg2.extras.execute_values(cur, "INSERT INTO carteira (vendedor, total_clientes, total_produtos, meta_faturamento) VALUES %s", data_tuples)
        
        conn.commit()
        cur.close()

        return jsonify({"message": "Arquivo de carteira processado com sucesso!"}), 200
    
    except Exception as e:
        print(f"Erro ao processar arquivo de carteira: {e}")
        return jsonify({"message": f"Erro ao processar arquivo: {e}"}), 500
    finally:
        if conn: conn.close()

@app.route('/api/dados', methods=['DELETE'])
def delete_data():
    """Apaga TODOS os dados das tabelas de vendas e carteira."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE vendas, carteira;")
        conn.commit()
        cur.close()
        return jsonify({"message": "Todos os dados foram apagados com sucesso."}), 200
    except Exception as e:
        print(f"Erro ao apagar dados: {e}")
        return jsonify({"message": f"Erro ao apagar dados: {e}"}), 500
    finally:
        if conn: conn.close()

