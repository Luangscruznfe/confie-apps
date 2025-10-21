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

# --- Inicialização do Flask App ---
app = Flask(__name__, static_folder='static', template_folder='templates', static_url_path='/dashboard/static')
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'sua-chave-secreta-padrao-aqui')
app.logger.addHandler(logging.StreamHandler(sys.stdout))
app.logger.setLevel(logging.INFO)

# --- Configuração do Login Manager ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'
login_manager.login_message = "Por favor, faça o login para acessar esta página."
login_manager.login_message_category = "info"

# --- Criação do Blueprint ---
dashboard_bp = Blueprint('dashboard_api', __name__)

# --- Funções Auxiliares ---
def get_db_connection():
    database_url = os.environ.get('DATABASE_URL')
    if database_url is None:
        raise ValueError("A variável de ambiente DATABASE_URL não foi encontrada.")
    conn = psycopg2.connect(database_url)
    return conn

class User(UserMixin):
    def __init__(self, id, username, role):
        self.id = id
        self.username = username
        self.role = role

@login_manager.user_loader
def load_user(user_id):
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT id, username, role FROM usuarios WHERE id = %s", (user_id,))
        user_data = cur.fetchone()
        cur.close()
    except Exception as e:
        app.logger.error(f"Erro ao carregar usuário {user_id}: {e}", exc_info=True)
        user_data = None
    finally:
        if conn: conn.close()

    if user_data:
        return User(id=user_data[0], username=user_data[1], role=user_data[2])
    return None

def count_weekdays(year, month, up_to_day=None):
    last_day = up_to_day if up_to_day is not None else calendar.monthrange(year, month)[1]
    count = 0
    for day in range(1, last_day + 1):
        try:
            current_date = datetime(year, month, day)
            if current_date.weekday() < 5:
                count += 1
        except ValueError:
            break
    return count


# --- Rotas Principais ---
@app.route("/login", methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard_page'))
    if request.method == 'POST':
        username = request.form.get('username', '').strip().upper()
        password = request.form.get('password')
        conn = None
        user_data = None
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT id, username, password_hash, role FROM usuarios WHERE username = %s", (username,))
            user_data = cur.fetchone()
            cur.close()
        except psycopg2.Error as e:
            app.logger.error(f"Erro no banco ao tentar login para {username}: {e}")
            flash('Erro ao conectar ao banco de dados.', 'danger')
        except Exception as e:
            app.logger.error(f"Erro inesperado durante login para {username}: {e}", exc_info=True)
            flash('Ocorreu um erro inesperado.', 'danger')
        finally:
            if conn: conn.close()

        if user_data and check_password_hash(user_data[2], password):
            user = User(id=user_data[0], username=user_data[1], role=user_data[3])
            login_user(user)
            app.logger.info(f"Usuário {username} logado com sucesso.")
            next_page = request.args.get('next')
            return redirect(next_page or url_for('dashboard_page'))
        else:
            app.logger.warning(f"Falha no login para o usuário {username}.")
            flash('Usuário ou senha inválidos.', 'danger')

    return render_template('login.html')

@app.route("/logout")
@login_required
def logout():
    logout_user()
    flash('Você saiu da sua conta.', 'success')
    return redirect(url_for('login'))

@app.route("/")
@login_required
def dashboard_page():
    last_update_str = "Nenhuma atualização encontrada."
    initial_month = datetime.now().strftime('%Y-%m')
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT MAX(data_venda) FROM public.vendas;")
        last_update_date = cur.fetchone()[0]
        if last_update_date:
            last_update_str = last_update_date.strftime('%d/%m/%Y')
            initial_month = last_update_date.strftime('%Y-%m')
        cur.close()
    except Exception as e:
        app.logger.error(f"Erro ao buscar data da última atualização/mês inicial: {e}")
    finally:
        if conn: conn.close()
    return render_template('dashboard.html', user_role=current_user.role, last_update_date=last_update_str, initial_month=initial_month)


# --- Rotas da API ---

@dashboard_bp.route("/api/limpar-dados", methods=['POST'])
@login_required
def delete_data():
    if current_user.role != 'admin':
        return jsonify({"message": "Acesso negado."}), 403
    conn = None
    try:
        conn = get_db_connection()
        conn.autocommit = False
        with conn.cursor() as cur:
            app.logger.info(f"Usuário {current_user.username} iniciando limpeza de dados.")
            cur.execute("TRUNCATE TABLE public.vendas RESTART IDENTITY;")
            cur.execute("TRUNCATE TABLE public.carteira RESTART IDENTITY;")
            cur.execute("TRUNCATE TABLE public.carteira_clientes RESTART IDENTITY;")
            conn.commit()
        app.logger.info("Limpeza de dados concluída com sucesso.")
        return jsonify({"message": "Todos os dados de vendas e carteira foram limpos com sucesso!"}), 200
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro ao limpar dados: {e}", exc_info=True)
        return jsonify({"message": f"Erro ao limpar dados: {str(e)}"}), 500
    finally:
        if conn:
            conn.autocommit = True
            conn.close()

@dashboard_bp.route("/api/upload/vendas", methods=['POST'])
@login_required
def upload_data():
    if current_user.role != 'admin':
        return jsonify({"message": "Acesso negado."}), 403
    if 'salesFile' not in request.files or 'portfolioFile' not in request.files or 'portfolioClientesFile' not in request.files:
        return jsonify({"message": "É necessário enviar os três arquivos: Vendas, Carteira (Resumo) e Carteira (Clientes)."}), 400

    sales_file = request.files['salesFile']
    portfolio_file = request.files['portfolioFile']
    portfolio_clientes_file = request.files['portfolioClientesFile']
    conn = None
    upload_month = None

    try:
        conn = get_db_connection()
        conn.autocommit = False

        # --- Processamento Vendas ---
        if not sales_file.filename.endswith(('.xlsx', '.csv')):
             raise ValueError("Formato de ficheiro de vendas inválido. Use .xlsx ou .csv")
        usecols_spec, col_names = "B,C,G,H,M,N,P,U,W", ['data_venda', 'nota_fiscal', 'cliente', 'nome_fantasia', 'produto', 'quantidade', 'valor', 'fabricante', 'vendedor']
        sales_file.stream.seek(0)
        sales_df = pd.read_excel(sales_file.stream, skiprows=9, usecols=usecols_spec, header=None, dtype={'cliente': str, 'nota_fiscal': str}) if sales_file.filename.endswith('.xlsx') else pd.read_csv(sales_file.stream, sep=';', skiprows=9, usecols=[1, 2, 6, 7, 12, 13, 15, 20, 22], header=None, encoding='latin1', dtype={'cliente': str, 'nota_fiscal': str})
        sales_df.columns = col_names
        sales_df['data_venda'] = pd.to_datetime(sales_df['data_venda'], dayfirst=True, errors='coerce')
        sales_df.dropna(subset=['data_venda'], inplace=True)
        if sales_df.empty: raise ValueError("O ficheiro de vendas não contém datas válidas.")
        upload_month = sales_df['data_venda'].dt.to_period('M').mode()[0].strftime('%Y-%m')
        for col in ['quantidade', 'valor']:
            if sales_df[col].dtype == 'object':
                sales_df[col] = sales_df[col].astype(str).str.replace(r'[^\d,.]', '', regex=True).str.replace(',', '.')
            sales_df[col] = pd.to_numeric(sales_df[col], errors='coerce')
        sales_df.dropna(subset=['valor', 'produto'], inplace=True)
        for col in ['cliente', 'nome_fantasia', 'produto', 'fabricante', 'vendedor', 'nota_fiscal']:
            if col in sales_df.columns:
                 sales_df[col] = sales_df[col].astype(str).str.strip().str.upper()
                 sales_df[col] = sales_df[col].replace({'NONE': None, 'NAN': None, '': None, 'NULL': None})

        with conn.cursor() as cur:
            app.logger.info(f"Deletando vendas do mês {upload_month}...")
            cur.execute("DELETE FROM public.vendas WHERE TO_CHAR(data_venda, 'YYYY-MM') = %s", (upload_month,))
            app.logger.info("Inserindo novas vendas...")
            data_to_insert_sales = [tuple(row) for row in sales_df[col_names].where(pd.notnull(sales_df[col_names]), None).itertuples(index=False)]
            sql_insert_sales = "INSERT INTO public.vendas (data_venda, nota_fiscal, cliente, nome_fantasia, produto, quantidade, valor, fabricante, vendedor) VALUES %s"
            execute_values(cur, sql_insert_sales, data_to_insert_sales, page_size=1000)
            app.logger.info(f"{len(data_to_insert_sales)} registros de vendas inseridos.")

        # --- Processamento Carteira Resumo ---
        if not portfolio_file.filename.endswith('.csv'):
             raise ValueError("Formato de ficheiro de carteira (resumo) inválido. Use .csv")
        portfolio_file.stream.seek(0)
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
            if portfolio_df[col].dtype == 'object':
                 portfolio_df[col] = portfolio_df[col].astype(str).str.replace(r'[^\d,.]', '', regex=True).str.replace(',', '.')
            portfolio_df[col] = pd.to_numeric(portfolio_df[col], errors='coerce')
        portfolio_df.dropna(subset=['vendedor'], inplace=True)
        portfolio_df['vendedor'] = portfolio_df['vendedor'].astype(str).str.strip().str.upper()
        portfolio_df['mes'] = upload_month
        df_for_db_portfolio = portfolio_df.astype(object).where(pd.notnull(portfolio_df), None)
        with conn.cursor() as cur:
            app.logger.info(f"Deletando carteira (resumo) do mês {upload_month}...")
            cur.execute("DELETE FROM public.carteira WHERE mes = %s", (upload_month,))
            app.logger.info("Inserindo nova carteira (resumo)...")
            sql_insert_portfolio = "INSERT INTO public.carteira (vendedor, total_clientes, total_produtos, meta_faturamento, mes) VALUES %s"
            execute_values(cur, sql_insert_portfolio, [tuple(row) for row in df_for_db_portfolio.itertuples(index=False)])
            app.logger.info(f"{len(df_for_db_portfolio)} registros de carteira (resumo) inseridos.")

        # --- Processamento Carteira Clientes ---
        if not portfolio_clientes_file.filename.endswith('.csv'):
             raise ValueError("Formato de ficheiro de carteira (clientes) inválido. Use .csv")
        portfolio_clientes_file.stream.seek(0)
        try:
            clientes_df = pd.read_csv(portfolio_clientes_file.stream, sep=';', encoding='latin1', header=0, keep_default_na=False, na_values=[''], on_bad_lines='warn', dtype=str)
            app.logger.info("Arquivo de clientes lido com separador ';'")
            if len(clientes_df.columns) == 1 and ';' in clientes_df.columns[0]:
                 app.logger.warning("Leitura com ';' resultou em uma única coluna. Tentando com ','.")
                 portfolio_clientes_file.stream.seek(0)
                 clientes_df = pd.read_csv(portfolio_clientes_file.stream, sep=',', encoding='latin1', header=0, keep_default_na=False, na_values=[''], on_bad_lines='warn', dtype=str)
                 app.logger.info("Arquivo de clientes lido com separador ','")
                 if len(clientes_df.columns) < 3: raise ValueError("Não foi possível separar as colunas do arquivo de clientes.")
        except Exception as e: raise ValueError(f"Erro ao ler o arquivo CSV de clientes: {e}")

        clientes_df.columns = clientes_df.columns.str.strip().str.lower()
        column_mapping = {'codigo': 'codigo_cliente', 'nome fantasia': 'nome_fantasia', 'vendedor': 'vendedor', 'mes': 'mes', 'mês': 'mes'}
        clientes_df.rename(columns=column_mapping, inplace=True)
        db_columns = ['vendedor', 'codigo_cliente', 'nome_fantasia', 'mes']
        missing_cols = [col for col in db_columns if col not in clientes_df.columns]
        if missing_cols: raise ValueError(f"Coluna(s) necessária(s) não encontrada(s): {missing_cols}. Colunas: {list(clientes_df.columns)}")

        clientes_df = clientes_df[db_columns]
        clientes_df.dropna(subset=['vendedor', 'codigo_cliente', 'mes'], inplace=True, how='any')
        clientes_df['mes'] = upload_month
        clientes_df['codigo_cliente'] = clientes_df['codigo_cliente'].astype(str).str.strip()
        clientes_df['vendedor'] = clientes_df['vendedor'].astype(str).str.strip().str.upper()
        clientes_df['nome_fantasia'] = clientes_df['nome_fantasia'].astype(str).str.strip()
        clientes_df = clientes_df.astype(object).where(pd.notnull(clientes_df), None)

        with conn.cursor() as cur:
            app.logger.info(f"Deletando carteira (clientes) do mês {upload_month}...")
            cur.execute("DELETE FROM public.carteira_clientes WHERE mes = %s", (upload_month,))
            app.logger.info("Inserindo nova carteira (clientes)...")
            sql_insert_clientes = "INSERT INTO public.carteira_clientes (vendedor, codigo_cliente, nome_fantasia, mes) VALUES %s"
            data_to_insert_clientes = [tuple(row) for row in clientes_df.itertuples(index=False)]
            execute_values(cur, sql_insert_clientes, data_to_insert_clientes, page_size=1000)
            app.logger.info(f"{len(data_to_insert_clientes)} registros de carteira (clientes) inseridos.")

        conn.commit()
        return jsonify({"message": "Arquivos processados e dados inseridos com sucesso!"}), 201
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro no upload: {e}", exc_info=True)
        return jsonify({"message": f"Erro no upload: {str(e)}"}), 500
    finally:
        if conn:
            conn.autocommit = True
            conn.close()


@dashboard_bp.route("/api/top-clientes", methods=['GET'])
@login_required
def get_top_clientes_data():
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        month_filter = request.args.get('month')
        if not month_filter: return jsonify({"message": "Mês é obrigatório"}), 400
        vendedores_loja_set = {'SHEILA', 'ROSANGEL', 'DELIVERY', 'CAIQUE', 'CONFIE'}
        vendedores_para_query_set = set()
        if current_user.role == 'admin':
            vendedores_selecionados = request.args.getlist('vendedor')
            if vendedores_selecionados:
                if 'LOJA' in vendedores_selecionados: vendedores_para_query_set.update(vendedores_loja_set)
                for vendedor in vendedores_selecionados:
                    if vendedor != 'LOJA': vendedores_para_query_set.add(vendedor)
        else: vendedores_para_query_set = {current_user.username}

        where_conditions = ["TO_CHAR(data_venda, 'YYYY-MM') = %s"]
        params = [month_filter]
        if vendedores_para_query_set:
            placeholders = ','.join(['%s'] * len(vendedores_para_query_set))
            where_conditions.append(f"vendedor IN ({placeholders})")
            params.extend(list(vendedores_para_query_set))

        where_clause = "WHERE " + " AND ".join(where_conditions)
        query = f""" SELECT nome_fantasia, SUM(valor) as total FROM public.vendas {where_clause}
                     AND nome_fantasia IS NOT NULL AND TRIM(nome_fantasia) <> ''
                     GROUP BY nome_fantasia ORDER BY total DESC LIMIT 5; """
        cur.execute(query, tuple(params))
        top_clientes = [{col.name: float(val) if isinstance(val, Decimal) else val for col, val in zip(cur.description, row)} for row in cur.fetchall()]
        cur.close()
        return jsonify(top_clientes)
    except Exception as e:
        app.logger.error(f"Erro ao buscar top clientes: {e}", exc_info=True)
        return jsonify({"message": f"Erro interno: {str(e)}"}), 500
    finally:
        if conn: conn.close()


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

        vendedores_filter_req = request.args.getlist('vendedor')
        vendedores_selecionados = []
        vendedores_para_query_vendas = set()
        vendedores_para_carteira_list = []

        vendedores_loja_set = {'SHEILA', 'ROSANGEL', 'DELIVERY', 'CAIQUE', 'CONFIE'}
        default_vendedores = []

        if current_user.role == 'admin':
            default_vendedores = ['MARCELO', 'EVERTON', 'MARCOS', 'PEDRO', 'RODOLFO', 'SILVANA', 'THYAGO', 'TIAGO', 'LUIZ']
            vendedores_selecionados = vendedores_filter_req if vendedores_filter_req else default_vendedores
            vendedores_para_carteira_list = vendedores_selecionados

            if 'LOJA' in vendedores_selecionados:
                vendedores_para_query_vendas.update(vendedores_loja_set)
            for vendedor in vendedores_selecionados:
                if vendedor != 'LOJA':
                    vendedores_para_query_vendas.add(vendedor)
            if not vendedores_filter_req:
                 vendedores_para_query_vendas = set()

        else:
            vendedores_selecionados = [current_user.username]
            vendedores_para_carteira_list = vendedores_selecionados
            vendedores_para_query_vendas = {current_user.username}

        results['selectedVendors'] = vendedores_selecionados

        # --- Clausula WHERE e Params para VENDAS ---
        where_conditions_vendas = ["TO_CHAR(data_venda, 'YYYY-MM') = %s"]
        params_vendas = [month_filter]
        if vendedores_para_query_vendas:
            placeholders = ','.join(['%s'] * len(vendedores_para_query_vendas))
            where_conditions_vendas.append(f"vendedor IN ({placeholders})")
            params_vendas.extend(list(vendedores_para_query_vendas))
        where_clause_vendas = "WHERE " + " AND ".join(where_conditions_vendas)

        # --- Clausula WHERE e Params para CARTEIRA ---
        where_conditions_carteira = ["c.mes = %s"]
        params_carteira = [month_filter]
        # Aplica filtro de vendedor na carteira APENAS se houver seleção explícita OU se for vendedor
        if vendedores_filter_req or current_user.role != 'admin':
            # Usa a lista da seleção original (pode incluir LOJA aqui, ok para carteira resumo)
            if vendedores_para_carteira_list:
                placeholders = ','.join(['%s'] * len(vendedores_para_carteira_list))
                where_conditions_carteira.append(f"c.vendedor IN ({placeholders})")
                params_carteira.extend(vendedores_para_carteira_list)
        where_clause_carteira = "WHERE " + " AND ".join(where_conditions_carteira)


        analysis_year, analysis_month = map(int, month_filter.split('-'))
        total_dias_uteis_mes = count_weekdays(analysis_year, analysis_month)

        cur.execute(f"SELECT MAX(data_venda) FROM public.vendas {where_clause_vendas}", tuple(params_vendas))
        last_sale_date_row = cur.fetchone()
        last_sale_date = last_sale_date_row[0] if last_sale_date_row and last_sale_date_row[0] else None
        dias_uteis_passados = count_weekdays(analysis_year, analysis_month, last_sale_date.day) if last_sale_date else 0

        cur.execute(f"SELECT COALESCE(SUM(valor), 0), COALESCE(COUNT(DISTINCT cliente), 0), COALESCE(COUNT(DISTINCT nota_fiscal), 0) FROM public.vendas {where_clause_vendas}", tuple(params_vendas))
        faturamento_total, total_clientes_atendidos, total_pedidos = cur.fetchone() or (0, 0, 0)

        ticket_medio = float(faturamento_total / total_pedidos) if total_pedidos > 0 else 0.0
        media_diaria_dias_uteis = float(faturamento_total / dias_uteis_passados) if dias_uteis_passados > 0 else 0.0

        # Positivação
        cur.execute(f"SELECT COALESCE(SUM(c.total_clientes), 0) FROM public.carteira c {where_clause_carteira}", tuple(params_carteira))
        total_clientes_carteira = cur.fetchone()[0] or 0
        clientes_nao_ativados = int(total_clientes_carteira) - int(total_clientes_atendidos)
        if clientes_nao_ativados < 0: clientes_nao_ativados = 0
        results['positivacaoGeral'] = {'ativados': int(total_clientes_atendidos), 'nao_ativados': clientes_nao_ativados}
        positivacao_media = (total_clientes_atendidos / total_clientes_carteira * 100) if total_clientes_carteira > 0 else 0.0

        results['kpi'] = {"faturamentoTotal": float(faturamento_total), "totalClientesAtendidos": total_clientes_atendidos, "ticketMedio": ticket_medio, "positivacaoMedia": positivacao_media, "projecaoFaturamento": media_diaria_dias_uteis * total_dias_uteis_mes}

        # Gráficos baseados em VENDAS
        query_mappings = {'topSellers': f"SELECT vendedor, SUM(valor) as total FROM public.vendas {where_clause_vendas} GROUP BY vendedor ORDER BY total DESC;", 'topManufacturers': f"SELECT fabricante, SUM(valor) as total FROM public.vendas {where_clause_vendas} GROUP BY fabricante ORDER BY total DESC LIMIT 10;", 'topProducts': f"SELECT produto, SUM(valor) as total FROM public.vendas {where_clause_vendas} GROUP BY produto ORDER BY total DESC LIMIT 10;"}
        for key, query in query_mappings.items():
            cur.execute(query, tuple(params_vendas))
            results[key] = [{col.name: float(val) if isinstance(val, Decimal) else val for col, val in zip(cur.description, row)} for row in cur.fetchall()]

        # Mix de Produtos (Não inclui LOJA)
        mix_params_carteira = [month_filter]
        mix_where_conditions_carteira = ["c.mes = %s"]
        vendedores_mix = [v for v in vendedores_para_carteira_list if v != 'LOJA']
        if vendedores_mix:
             placeholders = ','.join(['%s'] * len(vendedores_mix))
             mix_where_conditions_carteira.append(f"c.vendedor IN ({placeholders})")
             mix_params_carteira.extend(vendedores_mix)
        mix_where_clause_carteira = "WHERE " + " AND ".join(mix_where_conditions_carteira)
        query_product_mix = f"""
            WITH VendasProdutos AS (
                SELECT vendedor, COUNT(DISTINCT produto) as total
                FROM public.vendas {where_clause_vendas} GROUP BY vendedor
            )
            SELECT c.vendedor, COALESCE(vp.total, 0) as total
            FROM public.carteira c LEFT JOIN VendasProdutos vp ON c.vendedor = vp.vendedor
            {mix_where_clause_carteira} ORDER BY total DESC;
            """
        params_mix = tuple(params_vendas + mix_params_carteira)
        cur.execute(query_product_mix, params_mix)
        results['productMix'] = [{col.name: val for col, val in zip(cur.description, row)} for row in cur.fetchall()]


        # Fabricantes Foco
        fabricantes_foco = ['SELMI', 'LUCKY', 'RICLAN', 'KELLANOVA', 'TAMPICO', 'CONSABOR', 'YAI', 'TECPOLPA', 'GOLDKO']
        focus_where_conditions = where_conditions_vendas.copy()
        focus_params = list(params_vendas)
        placeholders_foco = ','.join(['%s'] * len(fabricantes_foco))
        focus_where_conditions.append(f"fabricante IN ({placeholders_foco})")
        focus_params.extend(fabricantes_foco)
        focus_where_clause = "WHERE " + " AND ".join(focus_where_conditions)
        cur.execute(f"SELECT fabricante, SUM(valor) as total FROM public.vendas {focus_where_clause} GROUP BY fabricante ORDER BY total DESC;", tuple(focus_params))
        results['focusManufacturers'] = [{col.name: float(val) if isinstance(val, Decimal) else val for col, val in zip(cur.description, row)} for row in cur.fetchall()]

        # --- Lógica de Metas ---
        vendedores_para_exibir_metas = []
        if current_user.role == 'admin':
             vendedores_para_exibir_metas = vendedores_filter_req if vendedores_filter_req else default_vendedores
             if not vendedores_filter_req:
                 cur.execute("SELECT 1 FROM public.carteira WHERE mes = %s AND vendedor = 'LOJA' LIMIT 1;", (month_filter,))
                 if cur.fetchone() and 'LOJA' not in vendedores_para_exibir_metas:
                     vendedores_para_exibir_metas.append('LOJA')
        else:
             vendedores_para_exibir_metas = [current_user.username]

        sales_goals = []
        if vendedores_para_exibir_metas:
            # Prepara parâmetros para a cláusula WHERE da carteira nesta query
            params_metas_carteira = [month_filter] # Param 3: month_filter para c.mes
            placeholders_metas = ','.join(['%s'] * len(vendedores_para_exibir_metas))
            where_metas_carteira = f"WHERE c.mes = %s AND c.meta_faturamento > 0 AND c.vendedor IN ({placeholders_metas})"
            params_metas_carteira.extend(vendedores_para_exibir_metas) # Params 4..N

            # Query para buscar metas e faturamento atual
            safe_vendedores_loja_sql = ["'" + v.replace("'", "''") + "'" for v in vendedores_loja_set]
            query_metas = f"""
                WITH VendasIndividuais AS (
                    SELECT vendedor, SUM(valor) as faturamento_atual
                    FROM public.vendas WHERE TO_CHAR(data_venda, 'YYYY-MM') = %s GROUP BY vendedor -- Param 1
                ), VendasLoja AS (
                    SELECT 'LOJA' as vendedor, COALESCE(SUM(valor), 0) as faturamento_atual
                    FROM public.vendas WHERE TO_CHAR(data_venda, 'YYYY-MM') = %s AND vendedor IN ({','.join(safe_vendedores_loja_sql)}) -- Param 2
                ), VendasCombinadas AS (
                    SELECT vendedor, faturamento_atual FROM VendasIndividuais WHERE vendedor <> 'LOJA'
                    UNION ALL SELECT vendedor, faturamento_atual FROM VendasLoja
                )
                SELECT c.vendedor, c.meta_faturamento as meta, COALESCE(vc.faturamento_atual, 0) as atual
                FROM public.carteira c LEFT JOIN VendasCombinadas vc ON c.vendedor = vc.vendedor
                {where_metas_carteira}; -- Params 3..N
            """
            # --- CORREÇÃO DA MONTAGEM DOS PARÂMETROS ---
            params_query_metas = tuple([month_filter, month_filter] + params_metas_carteira) # Usa a lista params_metas_carteira completa
            # --- FIM DA CORREÇÃO ---

            cur.execute(query_metas, params_query_metas)
            sales_goals_raw = [{col.name: val for col, val in zip(cur.description, row)} for row in cur.fetchall()]

            for row in sales_goals_raw:
                meta = float(row.get('meta') or 0); atual = float(row.get('atual') or 0)
                row['meta'], row['atual'] = meta, atual
                row['percentual'] = (atual / meta) * 100 if meta > 0 else 0.0
                restante = meta - atual
                dias_restantes = total_dias_uteis_mes - dias_uteis_passados
                row['venda_diaria'] = (restante / dias_restantes) if dias_restantes > 0 and restante > 0 else 0.0
                row['projecao'] = (atual / dias_uteis_passados) * total_dias_uteis_mes if dias_uteis_passados > 0 else 0.0
                sales_goals.append(row)

        results['salesGoals'] = sorted(sales_goals, key=lambda x: x.get('percentual', 0), reverse=True)


        # Lista de vendedores para o filtro
        cur.execute("SELECT DISTINCT vendedor FROM public.vendas WHERE vendedor IS NOT NULL AND TRIM(vendedor) <> '' ORDER BY vendedor;")
        all_vendors = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT 1 FROM public.carteira WHERE mes = %s AND vendedor = 'LOJA' LIMIT 1;", (month_filter,))
        if cur.fetchone() is not None:
            if 'LOJA' not in all_vendors: all_vendors.append('LOJA'); all_vendors.sort()
        results['allVendors'] = all_vendors
        cur.close()
        return jsonify(results)
    except IndexError as ie:
        app.logger.error(f"Erro de índice (provavelmente parâmetros vs placeholders): {ie}", exc_info=True)
        try:
             # Tentativa segura de logar para debug
             app.logger.error(f"Query Bruta (Metas): {query_metas}")
             app.logger.error(f"Parâmetros Tentados (Metas): {params_query_metas}")
             app.logger.error(f"Params Metas Carteira Base: {params_metas_carteira}")
        except Exception as log_e: app.logger.error(f"Erro ao logar query/params para IndexError: {log_e}")
        return jsonify({"message": f"Erro interno: Descompasso query/parâmetros ({str(ie)})"}), 500
    except Exception as e:
        app.logger.error(f"Erro crítico na função get_data: {e}", exc_info=True)
        return jsonify({"message": f"Erro interno: {str(e)}"}), 500
    finally:
        if conn: conn.close()


@dashboard_bp.route("/api/dados-cumulativos", methods=['GET'])
@login_required
def get_cumulative_data():
    conn = None
    try:
        conn = get_db_connection()
        months_to_compare = request.args.getlist('meses')
        if not months_to_compare: return jsonify({"message": "Mês obrigatório."}), 400

        params = []
        base_query = "SELECT EXTRACT(DAY FROM data_venda) as dia, TO_CHAR(data_venda, 'YYYY-MM') as mes, SUM(valor) as total_dia FROM public.vendas"
        where_conditions = []
        month_placeholders = ','.join(['%s'] * len(months_to_compare))
        where_conditions.append(f"TO_CHAR(data_venda, 'YYYY-MM') IN ({month_placeholders})")
        params.extend(months_to_compare)

        vendedores_para_query_set = set()
        if current_user.role == 'admin':
            vendedores_selecionados = request.args.getlist('vendedor')
            if vendedores_selecionados:
                vendedores_loja_set = {'SHEILA', 'ROSANGEL', 'DELIVERY', 'CAIQUE', 'CONFIE'}
                if 'LOJA' in vendedores_selecionados: vendedores_para_query_set.update(vendedores_loja_set)
                for vendedor in vendedores_selecionados:
                    if vendedor != 'LOJA': vendedores_para_query_set.add(vendedor)
        else:
            vendedores_para_query_set = {current_user.username}

        if vendedores_para_query_set:
            vendedor_placeholders = ','.join(['%s'] * len(vendedores_para_query_set))
            where_conditions.append(f"vendedor IN ({vendedor_placeholders})")
            params.extend(list(vendedores_para_query_set))

        final_query = f"{base_query} WHERE {' AND '.join(where_conditions)} GROUP BY 1, 2 ORDER BY 1, 2;"
        df = pd.read_sql_query(final_query, conn, params=params)

        if df.empty: return jsonify({"labels": list(range(1, 32)), "datasets": []})

        pivot_df = df.pivot_table(index='dia', columns='mes', values='total_dia', fill_value=0)
        all_month_cols = pd.to_datetime(months_to_compare).strftime('%Y-%m').tolist()
        pivot_df = pivot_df.reindex(columns=all_month_cols, fill_value=0)

        cumulative_df = pivot_df.cumsum().reindex(pd.Index(range(1, 32), name='dia')).ffill().fillna(0)
        datasets = []
        for month_col in all_month_cols:
            month_date = datetime.strptime(month_col, '%Y-%m')
            label = month_date.strftime('%B/%y').capitalize()
            if month_col in cumulative_df.columns:
                 datasets.append({"label": label, "data": cumulative_df[month_col].round(2).tolist()})
            else:
                 datasets.append({"label": label, "data": [0] * 31 })

        return jsonify({"labels": cumulative_df.index.tolist(), "datasets": datasets})
    except Exception as e:
        app.logger.error(f"Erro ao buscar dados cumulativos: {e}", exc_info=True)
        return jsonify({"message": f"Erro interno: {str(e)}"}), 500
    finally:
        if conn: conn.close()


@dashboard_bp.route("/api/clientes-nao-positivados", methods=['GET'])
@login_required
def get_clientes_nao_positivados():
    conn = None
    query = "" # Inicializa a variável query
    params = [] # Inicializa a variável params
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        month_filter = request.args.get('month')
        if not month_filter: return jsonify({"message": "Mês é obrigatório"}), 400

        vendedores_selecionados_req = request.args.getlist('vendedor')
        vendedores_para_consulta = []

        if current_user.role == 'admin':
            if not vendedores_selecionados_req:
                vendedores_para_consulta = ['MARCELO', 'EVERTON', 'MARCOS', 'PEDRO', 'RODOLFO', 'SILVANA', 'THYAGO', 'TIAGO', 'LUIZ']
                # Não inclui LOJA por padrão aqui
            else:
                vendedores_para_consulta = [v for v in vendedores_selecionados_req if v != 'LOJA']
        else:
            vendedores_para_consulta = [current_user.username]
            if 'LOJA' in vendedores_para_consulta: vendedores_para_consulta.remove('LOJA')

        if not vendedores_para_consulta:
             return jsonify([]) # Retorna lista vazia se só sobrou LOJA ou nenhum vendedor

        # --- Montagem da Query ---
        params = [month_filter] # 1º %s (vendas.data_venda)

        placeholders = ','.join(['%s'] * len(vendedores_para_consulta))
        where_vendas_clause = f"AND v.vendedor IN ({placeholders})"
        params.extend(vendedores_para_consulta) # Adiciona vendedores para vendas

        params.append(month_filter) # 2º %s (cc.mes)

        where_carteira_clause = f"AND cc.vendedor IN ({placeholders})"
        params.extend(vendedores_para_consulta) # Adiciona os MESMOS vendedores para carteira

        query = f"""
        SELECT cc.nome_fantasia, cc.codigo_cliente, cc.vendedor
        FROM public.carteira_clientes cc
        LEFT JOIN (
            SELECT DISTINCT v.cliente FROM public.vendas v
            WHERE TO_CHAR(v.data_venda, 'YYYY-MM') = %s {where_vendas_clause}
        ) AS clientes_positivados ON cc.codigo_cliente = clientes_positivados.cliente
        WHERE cc.mes = %s {where_carteira_clause} AND clientes_positivados.cliente IS NULL
        ORDER BY cc.vendedor, cc.nome_fantasia;
        """

        cur.execute(query, tuple(params))
        clientes = [{"nome_fantasia": row[0], "codigo_cliente": row[1], "vendedor": row[2]} for row in cur.fetchall()]
        cur.close()
        return jsonify(clientes)

    except psycopg2.Error as db_err:
         app.logger.error(f"Erro DB clientes não positivados: {db_err}", exc_info=True)
         failed_query_str = "Query não disponível (erro antes da execução?)"
         try: failed_query_str = cur.mogrify(query, tuple(params)) if cur else query + " | PARAMS: " + str(params)
         except: failed_query_str = query + " | PARAMS (falha mogrify): " + str(params)
         app.logger.error(f"Query falhou: {failed_query_str}")
         return jsonify({"message": f"Erro DB: {db_err.pgcode} - {db_err.diag.message_primary}"}), 500
    except Exception as e:
        app.logger.error(f"Erro inesperado clientes não positivados: {e}", exc_info=True)
        return jsonify({"message": f"Erro interno inesperado: {str(e)}"}), 500
    finally:
        if conn: conn.close()


# --- Registro Final do Blueprint ---
app.register_blueprint(dashboard_bp)

# --- Bloco Principal ---
if __name__ == "__main__":
    pass