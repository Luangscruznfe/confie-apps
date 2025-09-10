# dashboard_pbi/app.py

import pandas as pd
import plotly.express as px
from flask import Flask, request, render_template, flash

# Cria a instância da aplicação Flask para o dashboard
app = Flask(__name__)
# Chave secreta necessária para usar o 'flash' (mensagens de alerta)
app.secret_key = 'sua-chave-secreta-aqui' 

@app.route('/', methods=['GET', 'POST'])
def pagina_upload():
    if request.method == 'POST':
        # 1. Verifica se um arquivo foi enviado
        if 'file' not in request.files:
            flash('Nenhum arquivo enviado')
            return render_template('upload.html')

        file = request.files['file']

        # 2. Verifica se o nome do arquivo não está vazio
        if file.filename == '':
            flash('Nenhum arquivo selecionado')
            return render_template('upload.html')

        # 3. Se o arquivo existe e é um arquivo excel
        if file and (file.filename.endswith('.xlsx') or file.filename.endswith('.xls')):
            try:
                # 4. Lê o arquivo Excel com o Pandas
                df = pd.read_excel(file)

                # --- LÓGICA DE ANÁLISE E CRIAÇÃO DE GRÁFICOS ---
                # Esta parte agora está ADAPTADA para as colunas do seu relatório.
                
                # Exemplo 1: Gráfico de Barras - Vendas por Item
                fig_bar = px.bar(
                    df, 
                    x='ITENS',      # <-- CORRIGIDO
                    y='VENDA',      # <-- CORRIGIDO
                    title='Total de Venda por Item'
                )

                # Exemplo 2: Gráfico de Pizza - Vendas por Fabricante
                fig_pie = px.pie(
                    df, 
                    names='FABRICANTE', # <-- CORRIGIDO
                    values='VENDA',     # <-- CORRIGIDO
                    title='Distribuição de Venda por Fabricante'
                )
                
                # 5. Converte os gráficos para HTML
                grafico_bar_html = fig_bar.to_html(full_html=False)
                grafico_pie_html = fig_pie.to_html(full_html=False)

                # 6. Envia os gráficos para a página de dashboard
                return render_template(
                    'dashboard.html', 
                    grafico1_html=grafico_bar_html, 
                    grafico2_html=grafico_pie_html
                )

            except Exception as e:
                flash(f'Erro ao processar o arquivo: {e}')
                return render_template('upload.html')
        else:
            flash('Formato de arquivo inválido. Por favor, envie um arquivo .xlsx ou .xls')
            return render_template('upload.html')

    # Se a requisição for GET, apenas mostra a página de upload
    return render_template('upload.html')