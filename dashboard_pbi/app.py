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
                # O Pandas consegue ler o arquivo diretamente da memória, sem precisar salvar no disco
                df = pd.read_excel(file)

                # --- LÓGICA DE ANÁLISE E CRIAÇÃO DE GRÁFICOS ---
                # Esta parte é um EXEMPLO. Você precisará adaptar para as colunas do seu relatório.
                
                # Exemplo 1: Gráfico de Barras - Vendas por Produto
                # Supondo que seu Excel tenha as colunas 'Produto' e 'Vendas'
                fig_bar = px.bar(
                    df, 
                    x='Produto', 
                    y='Vendas', 
                    title='Total de Vendas por Produto'
                )

                # Exemplo 2: Gráfico de Pizza - Vendas por Categoria
                # Supondo que seu Excel tenha a coluna 'Categoria'
                fig_pie = px.pie(
                    df, 
                    names='Categoria', 
                    values='Vendas', 
                    title='Distribuição de Vendas por Categoria'
                )
                
                # 5. Converte os gráficos para HTML
                # Isso gera um <div> com o JavaScript necessário para o gráfico interativo
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