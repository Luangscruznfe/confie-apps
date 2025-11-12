# financeiro_app/app.py

import flask
import dash
from dash import dcc, html, Input, Output
import pandas as pd
from datetime import datetime
import plotly.graph_objects as go
import dash_bootstrap_components as dbc

# --- Bloco de Carregamento de Dados (Contas a Receber) ---
# (Oculto para economizar espaço, é o mesmo de antes)
try:
    df_cr = pd.read_csv('contas a receber.xlsx - Planilha1.csv')
    df_cr['VALOR'] = pd.to_numeric(df_cr['VALOR'], errors='coerce')
    df_cr['VENCTO'] = pd.to_datetime(df_cr['VENCTO'], format='%d.%m.%y', errors='coerce')
    
    hoje = datetime.now()
    df_cr['DIAS_ATRASO'] = (hoje - df_cr['VENCTO']).dt.days
    
    def categorizar_atraso(dias):
        if pd.isnull(dias) or dias < 0:
            return "A Vencer"
        elif 0 <= dias <= 15:
            return "0-15 dias"
        elif 16 <= dias <= 30:
            return "16-30 dias"
        elif 31 <= dias <= 60:
            return "31-60 dias"
        else:
            return "> 60 dias"
            
    df_cr['FAIXA_ATRASO'] = df_cr['DIAS_ATRASO'].apply(categorizar_atraso)
    
    cr_data_message = f"Dados de Contas a Receber carregados com sucesso ({len(df_cr)} linhas)."
    
    # Preparação do Gráfico Aging List
    aging_data = df_cr.groupby('FAIXA_ATRASO')['VALOR'].sum().reset_index()
    ordem_faixas = ["A Vencer", "0-15 dias", "16-30 dias", "31-60 dias", "> 60 dias"]
    aging_data['FAIXA_ATRASO'] = pd.Categorical(aging_data['FAIXA_ATRASO'], categories=ordem_faixas, ordered=True)
    aging_data = aging_data.sort_values('FAIXA_ATRASO')
    
    fig_aging = go.Figure(data=[
        go.Bar(
            x=aging_data['FAIXA_ATRASO'], 
            y=aging_data['VALOR'],
            text=aging_data['VALOR'].apply(lambda x: f'R$ {x:,.2f}'),
            textposition='auto'
        )
    ])
    fig_aging.update_layout(
        title='Valor Total a Receber por Faixa de Atraso (Aging List)',
        xaxis_title='Faixa de Atraso',
        yaxis_title='Valor Total (R$)',
        yaxis=dict(tickformat=',.2f')
    )
    
except FileNotFoundError:
    df_cr = pd.DataFrame() 
    fig_aging = go.Figure()
    cr_data_message = "Erro: Arquivo 'contas a receber.xlsx - Planilha1.csv' não encontrado."
except Exception as e:
    df_cr = pd.DataFrame()
    fig_aging = go.Figure()
    cr_data_message = f"Erro ao processar o arquivo: {str(e)}"
# --- Fim do Bloco de Dados ---


# --- 1. Layouts das Páginas (Atualizados com a sintaxe correta) ---

# Layout da Home Financeiro
layout_home = dbc.Container([
    html.H1("Home Financeiro - Confie", className="my-4 text-center"),
    html.P("Selecione o módulo que deseja acessar:", className="lead text-center mb-4"),
    
    dbc.Row([
        # Card 1: Contas a Receber
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H4("Contas a Receber", className="card-title"),
                html.P("Análise de títulos em aberto, aging list e top devedores.", className="card-text"),
                # --- CORREÇÃO AQUI ---
                # O dcc.Link agora envolve o dbc.Button
                dcc.Link(
                    dbc.Button("Acessar Módulo", color="primary", className="w-100"), 
                    href='/financeiro/contas-a-receber',
                    style={'textDecoration': 'none'} # Remove sublinhado do link
                )
                # --- FIM DA CORREÇÃO ---
            ])
        ]), md=6, lg=4, className="mb-3"),

        # Card 2: Liquidados
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H4("Análise de Liquidados", className="card-title"),
                html.P("Perfil de pagamento de clientes, histórico e PMR.", className="card-text"),
                # --- CORREÇÃO AQUI ---
                dcc.Link(
                    dbc.Button("Acessar Módulo", color="success", className="w-100"), 
                    href='/financeiro/liquidados',
                    style={'textDecoration': 'none'}
                )
                # --- FIM DA CORREÇÃO ---
            ])
        ]), md=6, lg=4, className="mb-3"),
        
        # Card 3: Placeholder para futuro
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H4("Contas a Pagar (Em Breve)", className="card-title"),
                html.P("Gestão e previsão de pagamentos futuros.", className="card-text"),
                dbc.Button("Acessar Módulo", color="secondary", disabled=True, className="w-100")
            ])
        ]), md=6, lg=4, className="mb-3")

    ], justify="center")
], fluid=False)

# Layout dos Liquidados
layout_liquidados = dbc.Container([
    html.H1("Análise de Perfil de Pagamento (Liquidados)"),
    html.P("Aqui ficarão os dashboards de histórico, PMR, tendências, etc."),
    html.Hr(),
    html.P("Página em construção."),
    # --- CORREÇÃO AQUI ---
    dcc.Link(
        dbc.Button("Voltar para Home", color="secondary"), 
        href="/financeiro/",
        style={'textDecoration': 'none'}
    )
    # --- FIM DA CORREÇÃO ---
], fluid=True)

# Layout de Contas a Receber
layout_contas_a_receber = dbc.Container([
    html.H1("Dashboard de Contas a Receber"),
    html.P(cr_data_message),
    # --- CORREÇÃO AQUI ---
    dcc.Link(
        dbc.Button("Voltar para Home", color="secondary"), 
        href="/financeiro/",
        style={'textDecoration': 'none'},
        className="mb-3" # Adiciona margem inferior
    ),
    # --- FIM DA CORREÇÃO ---
    html.Hr(),
    dbc.Row([
        dbc.Col(dcc.Graph(id='graph-aging-list', figure=fig_aging))
    ])
], fluid=True)


# --- 2. Criação do App Dash ÚNICO ---
app = dash.Dash(
    __name__,
    requests_pathname_prefix='/financeiro/',
    suppress_callback_exceptions=True,
    external_stylesheets=[dbc.themes.FLATLY]
)

server = app.server 

app.layout = html.Div([
    dcc.Location(id='url-financeiro', refresh=False),
    html.Div(id='page-content-financeiro')
])

# --- 3. Callback de Roteamento ---
@app.callback(
    Output('page-content-financeiro', 'children'),
    [Input('url-financeiro', 'pathname')]
)
def display_page(pathname):
    if pathname == '/financeiro/contas-a-receber':
        return layout_contas_a_receber
    elif pathname == '/financeiro/liquidados':
        return layout_liquidados
    elif pathname == '/financeiro/' or pathname == '/financeiro':
        return layout_home
    else:
        return html.H3(f"Erro 404 - Página não encontrada: {pathname}")

    
# --- 4. Bloco de Teste ---
if __name__ == '__main__':
    app.run_server(debug=True, port=8050)