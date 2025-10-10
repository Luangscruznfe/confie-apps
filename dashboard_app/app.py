import os
import psycopg2
import pandas as pd
from flask import Flask, jsonify, render_template, request, Blueprint, redirect, url_for, flash
from werkzeug.security import check_password_hash
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
import csv
import io
from psycopg2.extras import execute_values
from decimal import Decimal
import logging
import sys
import re
from dotenv import load_dotenv
from datetime import datetime
import calendar
from dateutil.relativedelta import relativedelta

load_dotenv()

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
app = Flask(__name__, static_folder='static', template_folder='templates')
# --- NOVO: Chave secreta para gerenciar sessões de login ---
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'sua-chave-secreta-padrao-aqui')
app.logger.addHandler(logging.StreamHandler(sys.stdout))
app.logger.setLevel(logging.INFO)

# --- NOVO: Configuração do Flask-Login ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login' # Rota para a qual usuários não logados são redirecionados
login_manager.login_message = "Por favor, faça o login para acessar esta página."
login_manager.login_message_category = "info"

dashboard_bp = Blueprint('dashboard_api', __name__)

def get_db_connection():
    database_url = os.environ.get('DATABASE_URL')
    if database_url is None:
        raise ValueError("A variável de ambiente DATABASE_URL não foi encontrada.")
    conn = psycopg2.connect(database_url)
    return conn

# --- NOVO: Classe de usuário para o Flask-Login ---
class User(UserMixin):
    def __init__(self, id, username, role):
        self.id = id
        self.username = username
        self.role = role

# --- NOVO: Carrega o usuário da sessão ---
@login_manager.user_loader
def load_user(user_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, username, role FROM usuarios WHERE id = %s", (user_id,))
    user_data = cur.fetchone()
    cur.close()
    conn.close()
    if user_data:
        return User(id=user_data[0], username=user_data[1], role=user_data[2])
    return None

def count_weekdays(year, month, up_to_day=None):
    # (Sua função original, sem alterações)
    last_day = up_to_day if up_to_day is not None else calendar.monthrange(year, month)[1]
    count = 0
    for day in range(1, last_day + 1):
        if day > calendar.monthrange(year, month)[1]: break
        if datetime(year, month, day).weekday() < 5: count += 1
    return count

# --- NOVO: Rota de Login ---
@app.route("/login", methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard_page'))
    
    if request.method == 'POST':
        username = request.form.get('username', '').upper()
        password = request.form.get('password')
        
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT id, username, password_hash, role FROM usuarios WHERE username = %s", (username,))
        user_data = cur.fetchone()
        cur.close()
        conn.close()
        
        if user_data and check_password_hash(user_data[2], password):
            user = User(id=user_data[0], username=user_data[1], role=user_data[3])
            login_user(user)
            return redirect(url_for('dashboard_page'))
        else:
            flash('Usuário ou senha inválidos.', 'danger')

    return render_template('login.html')

# --- NOVO: Rota de Logout ---
@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

# --- ALTERADO: Rota do Dashboard agora exige login ---
@app.route("/")
@login_required
def dashboard_page():
    # Passa a role do usuário para o template para controlar o que é exibido
    return render_template('dashboard.html', user_role=current_user.role)

# --- ALTERADO: Rota protegida e com verificação de permissão ---
@dashboard_bp.route("/api/limpar-dados", methods=['POST'])
@login_required
def delete_data():
    if current_user.role != 'admin':
        return jsonify({"message": "Acesso negado. Permissão de administrador necessária."}), 403
    
    # (Sua função original, sem alterações)
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE public.vendas RESTART IDENTITY;")
            cur.execute("TRUNCATE TABLE public.carteira RESTART IDENTITY;")
            conn.commit()
            app.logger.info("Todos os dados das tabelas 'vendas' e 'carteira' foram apagados.")
        return jsonify({"message": "Dados limpos com sucesso!"}), 200
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro ao limpar dados: {e}", exc_info=True)
        return jsonify({"message": f"Erro interno no servidor: {str(e)}"}), 500
    finally:
        if conn: conn.close()

# --- ALTERADO: Rota protegida e com verificação de permissão ---
@dashboard_bp.route("/api/upload/vendas", methods=['POST'])
@login_required
def upload_data():
    if current_user.role != 'admin':
        return jsonify({"message": "Acesso negado. Permissão de administrador necessária."}), 403

    # (Sua função original, sem alterações)
    if 'salesFile' not in request.files or 'portfolioFile' not in request.files:
        return jsonify({"message": "Ficheiros de vendas e carteira são obrigatórios."}), 400
    
    sales_file = request.files['salesFile']
    portfolio_file = request.files['portfolioFile']
    
    conn = None
    try:
        conn = get_db_connection()

        if not sales_file.filename.endswith(('.xlsx', '.csv')):
            return jsonify({"message": "Formato de ficheiro de vendas inválido."}), 400

        app.logger.info(f"A processar o ficheiro de vendas: {sales_file.filename}")
        usecols_spec, col_names = "B,G,M,N,P,U,W", ['data_venda', 'cliente', 'produto', 'quantidade', 'valor', 'fabricante', 'vendedor']
        
        sales_file.stream.seek(0)
        sales_df = pd.read_excel(sales_file.stream, skiprows=9, usecols=usecols_spec, header=None) if sales_file.filename.endswith('.xlsx') else pd.read_csv(sales_file.stream, sep=';', skiprows=9, usecols=[1, 6, 12, 13, 15, 20, 22], header=None, encoding='latin1')
        
        sales_df.columns = col_names
        sales_df['data_venda'] = pd.to_datetime(sales_df['data_venda'], dayfirst=True, errors='coerce')
        sales_df.dropna(subset=['data_venda'], inplace=True)

        if sales_df.empty: return jsonify({"message": "O ficheiro de vendas não contém datas válidas."}), 400
        
        upload_month = sales_df['data_venda'].dt.to_period('M').mode()[0].strftime('%Y-%m')
        app.logger.info(f"Mês de referência para este upload: {upload_month}")

        app.logger.info(f"Lendo o ficheiro da carteira: {portfolio_file.filename}")
        portfolio_df = pd.read_csv(portfolio_file.stream, sep=';', encoding='latin1')
        first_col_name = portfolio_df.columns[0]
        if ';' in first_col_name:
            split_data = portfolio_df[first_col_name].str.split(';', n=1, expand=True)
            portfolio_df['vendedor'], portfolio_df['total_clientes'] = split_data[0].str.strip('" '), split_data[1].str.strip('" ')
            portfolio_df.rename(columns={portfolio_df.columns[1]: 'total_produtos', portfolio_df.columns[2]: 'meta_faturamento'}, inplace=True)
            portfolio_df = portfolio_df[['vendedor', 'total_clientes', 'total_produtos', 'meta_faturamento']]
        else:
            portfolio_df.rename(columns={portfolio_df.columns[0]: 'vendedor', portfolio_df.columns[1]: 'total_clientes', portfolio_df.columns[2]: 'total_produtos', portfolio_df.columns[3]: 'meta_faturamento'}, inplace=True)
        
        for col in ['total_clientes', 'total_produtos', 'meta_faturamento']:
            portfolio_df[col] = pd.to_numeric(portfolio_df[col], errors='coerce')
        portfolio_df.dropna(subset=['vendedor'], inplace=True)
        portfolio_df['mes'] = upload_month
        
        df_for_db = portfolio_df.astype(object).where(pd.notnull(portfolio_df), None)

        with conn.cursor() as cur:
            cur.execute("DELETE FROM public.carteira WHERE mes = %s", (upload_month,))
            sql_insert_portfolio = "INSERT INTO public.carteira (vendedor, total_clientes, total_produtos, meta_faturamento, mes) VALUES %s"
            execute_values(cur, sql_insert_portfolio, [tuple(row) for row in df_for_db.itertuples(index=False)])
            app.logger.info(f"{len(portfolio_df)} registos da carteira inseridos para {upload_month}.")

        for col in ['quantidade', 'valor']:
            if sales_df[col].dtype == 'object':
                sales_df[col] = sales_df[col].astype(str).str.replace(r'[^\d,]', '', regex=True).str.replace(',', '.')
            sales_df[col] = pd.to_numeric(sales_df[col], errors='coerce')
        sales_df.dropna(subset=['valor', 'produto'], inplace=True)

        with conn.cursor() as cur:
            cur.execute("DELETE FROM public.vendas WHERE TO_CHAR(data_venda, 'YYYY-MM') = %s", (upload_month,))
            app.logger.info(f"Dados de vendas para o mês {upload_month} foram limpos para atualização.")
            
            data_to_insert = [tuple(row) for row in sales_df[col_names].itertuples(index=False)]
            sql_insert_sales = "INSERT INTO public.vendas (data_venda, cliente, produto, quantidade, valor, fabricante, vendedor) VALUES %s"
            execute_values(cur, sql_insert_sales, data_to_insert, page_size=1000)
            app.logger.info(f"{len(data_to_insert)} registos de vendas inseridos para {upload_month}.")

        conn.commit()
        return jsonify({"message": f"{len(data_to_insert)} registos inseridos!"}), 201
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro no upload dos dados: {e}", exc_info=True)
        return jsonify({"message": f"Erro ao processar os ficheiros: {str(e)}"}), 500
    finally:
        if conn: conn.close()

# --- ALTERADO: Rota de dados agora filtra por permissão do usuário ---
@dashboard_bp.route("/api/dados", methods=['GET'])
@login_required
def get_data():
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        results = {}

        month_filter = request.args.get('month')
        if not month_filter:
            cur.execute("SELECT TO_CHAR(MAX(data_venda), 'YYYY-MM') FROM public.vendas;")
            max_month_row = cur.fetchone()
            month_filter = (max_month_row[0] if max_month_row and max_month_row[0] else datetime.now().strftime('%Y-%m'))
        
        if current_user.role == 'admin':
            vendedores_filter_req = request.args.getlist('vendedor')
            default_vendedores = ['MARCELO', 'EVERTON', 'MARCOS', 'PEDRO', 'RODOLFO', 'SILVANA', 'THYAGO', 'TIAGO', 'LUIZ']
            vendedores_filter = vendedores_filter_req if vendedores_filter_req else default_vendedores
        else:
            vendedores_filter = [current_user.username]
        
        results['selectedVendors'] = vendedores_filter

        where_conditions = ["EXTRACT(ISODOW FROM data_venda) < 6"]
        where_conditions.append(f"TO_CHAR(data_venda, 'YYYY-MM') = '{month_filter}'")
        
        # Cria a lista de vendedores seguros para usar em cláusulas IN
        safe_vendedores = []
        if vendedores_filter:
            safe_vendedores = ["'" + v.replace("'", "''") + "'" for v in vendedores_filter]
            where_conditions.append(f"vendedor IN ({','.join(safe_vendedores)})")
            
        where_clause = "WHERE " + " AND ".join(where_conditions)

        today = datetime.now()
        analysis_year, analysis_month = map(int, month_filter.split('-'))
        total_dias_uteis_mes = count_weekdays(analysis_year, analysis_month)
        dias_uteis_passados = count_weekdays(analysis_year, analysis_month, today.day) if analysis_year == today.year and analysis_month == today.month else total_dias_uteis_mes

        cur.execute(f"SELECT COALESCE(SUM(valor), 0), COALESCE(COUNT(DISTINCT cliente), 0), COALESCE(COUNT(*), 0) FROM public.vendas {where_clause};")
        faturamento_total, total_clientes_atendidos, total_vendas = cur.fetchone() or (0, 0, 0)
        ticket_medio = float(faturamento_total / total_vendas) if total_vendas > 0 else 0.0
        media_diaria = float(faturamento_total / dias_uteis_passados) if dias_uteis_passados > 0 else 0.0
        
        positivacao_carteira_where = f"WHERE Carteira.mes = '{month_filter}'"
        if vendedores_filter: positivacao_carteira_where += f" AND Carteira.vendedor IN ({','.join(safe_vendedores)})"
        cur.execute(f"""
            WITH VendasPorVendedor AS (SELECT vendedor, COUNT(DISTINCT cliente) AS clientes_ativados FROM public.vendas {where_clause} GROUP BY vendedor)
            SELECT COALESCE(AVG((COALESCE(VendasPorVendedor.clientes_ativados, 0)::DECIMAL / Carteira.total_clientes) * 100), 0)
            FROM public.carteira AS Carteira LEFT JOIN VendasPorVendedor ON Carteira.vendedor = VendasPorVendedor.vendedor {positivacao_carteira_where} AND Carteira.total_clientes > 0;
        """)
        positivacao_result = cur.fetchone()
        positivacao_media = float(positivacao_result[0]) if positivacao_result and positivacao_result[0] is not None else 0.0
        
        results['kpi'] = {"faturamentoTotal": float(faturamento_total), "totalClientesAtendidos": total_clientes_atendidos, "ticketMedio": ticket_medio, "positivacaoMedia": positivacao_media, "projecaoFaturamento": media_diaria * total_dias_uteis_mes}

        query_mappings = {
            'topSellers': "SELECT vendedor, SUM(valor) as total FROM public.vendas {where_clause} GROUP BY vendedor ORDER BY total DESC;",
            'topManufacturers': "SELECT fabricante, SUM(valor) as total FROM public.vendas {where_clause} GROUP BY fabricante ORDER BY total DESC LIMIT 10;",
            'topProducts': "SELECT produto, SUM(valor) as total FROM public.vendas {where_clause} GROUP BY produto ORDER BY total DESC LIMIT 10;",
        }
        for key, query in query_mappings.items():
            cur.execute(query.format(where_clause=where_clause))
            results[key] = [{col.name: float(val) if isinstance(val, Decimal) else val for col, val in zip(cur.description, row)} for row in cur.fetchall()]

        cur.execute(f"""
            WITH VendasPorVendedor AS (SELECT vendedor, COUNT(DISTINCT cliente) AS clientes_ativados FROM public.vendas {where_clause} GROUP BY vendedor)
            SELECT c.vendedor, CASE WHEN c.total_clientes > 0 THEN (COALESCE(vpv.clientes_ativados, 0)::DECIMAL / c.total_clientes) * 100 ELSE 0 END AS positivacao
            FROM public.carteira AS c LEFT JOIN VendasPorVendedor vpv ON c.vendedor = vpv.vendedor {positivacao_carteira_where.replace('Carteira', 'c')} ORDER BY 2 DESC;
        """)
        results['positivacaoPorVendedor'] = [{col.name: float(val) if isinstance(val, Decimal) else val for col, val in zip(cur.description, row)} for row in cur.fetchall()]
        
        cur.execute(f"""
            WITH ProdutosVendidos AS (SELECT vendedor, COUNT(DISTINCT produto) AS produtos_unicos_vendidos FROM public.vendas {where_clause} GROUP BY vendedor)
            SELECT c.vendedor, CASE WHEN c.total_produtos > 0 THEN (COALESCE(pv.produtos_unicos_vendidos, 0)::DECIMAL / c.total_produtos) * 100 ELSE 0 END AS mix
            FROM public.carteira AS c LEFT JOIN ProdutosVendidos pv ON c.vendedor = pv.vendedor WHERE c.mes = '{month_filter}' AND c.total_produtos > 0 ORDER BY 2 DESC;
        """)
        results['productMix'] = [{col.name: float(val) if isinstance(val, Decimal) else val for col, val in zip(cur.description, row)} for row in cur.fetchall()]

        fabricantes_foco = ['SELMI', 'LUCKY', 'RICLAN', 'KELLANOVA', 'TAMPICO', 'CONSABOR', 'YAI', 'TECPOLPA', 'GOLDKO']
        focus_where = where_clause + f" AND fabricante IN ({','.join(['%s'] * len(fabricantes_foco))})"
        cur.execute(f"SELECT fabricante, SUM(valor) as total FROM public.vendas {focus_where} GROUP BY fabricante ORDER BY total DESC;", fabricantes_foco)
        results['focusManufacturers'] = [{col.name: float(val) if isinstance(val, Decimal) else val for col, val in zip(cur.description, row)} for row in cur.fetchall()]

        # --- INÍCIO DA CORREÇÃO ---
        # Constrói a cláusula WHERE para a consulta principal da carteira
        carteira_where_conditions = [
            f"c.mes = '{month_filter}'",
            "c.meta_faturamento > 0"
        ]
        if vendedores_filter:
            carteira_where_conditions.append(f"c.vendedor IN ({','.join(safe_vendedores)})")
        
        carteira_where_clause = " AND ".join(carteira_where_conditions)

        # A consulta agora usa a nova cláusula `carteira_where_clause` para filtrar a tabela de carteira
        cur.execute(f"""
            WITH VendasAtuais AS (SELECT vendedor, SUM(valor) as faturamento_atual FROM public.vendas {where_clause} GROUP BY vendedor)
            SELECT c.vendedor, c.meta_faturamento as meta, COALESCE(va.faturamento_atual, 0) as atual
            FROM public.carteira c LEFT JOIN VendasAtuais va ON c.vendedor = va.vendedor
            WHERE {carteira_where_clause};
        """)
        # --- FIM DA CORREÇÃO ---
        
        sales_goals_raw = [{col.name: val for col, val in zip(cur.description, row)} for row in cur.fetchall()]
        sales_goals = []
        for row in sales_goals_raw:
            meta = float(row.get('meta') or 0)
            atual = float(row.get('atual') or 0)
            row['meta'], row['atual'] = meta, atual
            row['percentual'] = (atual / meta) * 100 if meta > 0 else 0.0
            restante = meta - atual
            row['venda_diaria'] = (restante / (total_dias_uteis_mes - dias_uteis_passados)) if (total_dias_uteis_mes - dias_uteis_passados) > 0 and restante > 0 else 0.0
            row['projecao'] = (atual / dias_uteis_passados) * total_dias_uteis_mes if dias_uteis_passados > 0 else 0.0
            sales_goals.append(row)
        results['salesGoals'] = sorted(sales_goals, key=lambda x: x['percentual'], reverse=True)
        
        cur.execute("""
            SELECT DISTINCT vendedor FROM public.vendas
            WHERE vendedor IS NOT NULL AND TRIM(vendedor) <> '' ORDER BY vendedor;
        """)
        results['allVendors'] = [r[0] for r in cur.fetchall()]
        
        cur.close()
        return jsonify(results)
    except Exception as e:
        app.logger.error(f"Erro crítico na função get_data: {e}", exc_info=True)
        return jsonify({"message": f"Erro interno: {str(e)}"}), 500
    finally:
        if conn: conn.close()

# --- ALTERADO: Rota protegida por login ---
@dashboard_bp.route("/api/dados-cumulativos", methods=['GET'])
@login_required
def get_cumulative_data():
    conn = None
    try:
        conn = get_db_connection()
        months_to_compare = request.args.getlist('meses')
        if not months_to_compare: 
            return jsonify({"message": "Pelo menos um mês deve ser fornecido."}), 400

        # --- INÍCIO DA CORREÇÃO ---
        
        # Prepara a lista de parâmetros para a consulta SQL
        params = list(months_to_compare)
        
        # Constrói a consulta dinamicamente
        base_query = """
            SELECT 
                EXTRACT(DAY FROM data_venda) as dia,
                TO_CHAR(data_venda, 'YYYY-MM') as mes,
                SUM(valor) as total_dia
            FROM public.vendas
        """
        
        where_conditions = []
        
        # Adiciona o filtro de mês, que sempre existe
        month_placeholders = ','.join(['%s'] * len(months_to_compare))
        where_conditions.append(f"TO_CHAR(data_venda, 'YYYY-MM') IN ({month_placeholders})")
        
        # Adiciona o filtro de vendedor APENAS se o usuário for um vendedor
        if current_user.role == 'vendedor':
            where_conditions.append("vendedor = %s")
            params.append(current_user.username) # Adiciona o nome do vendedor aos parâmetros

        # Monta a query final
        final_query = f"{base_query} WHERE {' AND '.join(where_conditions)} GROUP BY 1, 2 ORDER BY 1, 2;"

        # Executa a consulta com os parâmetros corretos
        df = pd.read_sql_query(final_query, conn, params=params)
        
        # --- FIM DA CORREÇÃO ---
        
        if df.empty: 
            return jsonify({"labels": list(range(1, 32)), "datasets": []})

        pivot_df = df.pivot_table(index='dia', columns='mes', values='total_dia', fill_value=0)
        cumulative_df = pivot_df.cumsum().reindex(pd.Index(range(1, 32), name='dia')).ffill().fillna(0)
        
        datasets = []
        for month_col in cumulative_df.columns:
            month_date = datetime.strptime(month_col, '%Y-%m')
            label = month_date.strftime('%B/%y').capitalize()
            datasets.append({
                "label": label,
                "data": cumulative_df[month_col].round(2).tolist()
            })
            
        return jsonify({"labels": cumulative_df.index.tolist(), "datasets": datasets})
    except Exception as e:
        app.logger.error(f"Erro ao buscar dados cumulativos: {e}", exc_info=True)
        return jsonify({"message": f"Erro interno: {str(e)}"}), 500
    finally:
        if conn: conn.close()

app.register_blueprint(dashboard_bp)

if __name__ == "__main__":
    # O host 0.0.0.0 é importante para acessar de outros dispositivos na mesma rede
    app.run(debug=True, host='0.0.0.0', port=8000)