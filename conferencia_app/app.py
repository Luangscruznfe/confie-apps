# =================================================================
# APP.PY (VERSÃO DE DIAGNÓSTICO PARA /mapa/extrator)
# =================================================================
# 1. IMPORTAÇÕES
# =================================================================
from flask import Flask, jsonify, render_template, abort, request, Response, redirect, flash, url_for
import cloudinary, cloudinary.uploader, cloudinary.api
import psycopg2, psycopg2.extras
import json, os, re, io, fitz, shutil, requests
from werkzeug.utils import secure_filename
from collections import defaultdict
from datetime import datetime
from zipfile import ZipFile
import pandas as pd
import sys
import logging

# Tenta importar o parser principal
try:
    from conferencia_app.parser_mapa import parse_mapa
except ImportError:
    try:
        from .parser_mapa import parse_mapa
    except ImportError:
        # Se não encontrar, define uma função placeholder para não quebrar o app
        def parse_mapa(pdf_path):
            return {}, None, [], []


# =================================================================
# 2. CONFIGURAÇÃO DA APP FLASK
# =================================================================
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "confie123")


# =================================================================
# 3. FUNÇÕES AUXILIARES E DE BANCO DE DADOS (Omitidas para brevidade, use as suas)
# =================================================================

def get_db_connection():
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
    return conn

# ... (Mantenha todas as suas funções de DB, extrair_dados_do_pdf, etc., aqui) ...
# O código omitido é o mesmo que você já tem no seu app.py original.
# A única parte que realmente importa para este teste é a rota /mapa/extrator no final.

#<editor-fold desc="Funções de DB e Rotas Padrão (COPIE DO SEU ARQUIVO ORIGINAL)">
def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS pedidos (
            id SERIAL PRIMARY KEY, numero_pedido TEXT UNIQUE NOT NULL, nome_cliente TEXT, vendedor TEXT,
            nome_da_carga TEXT, nome_arquivo TEXT, status_conferencia TEXT, produtos JSONB, url_pdf TEXT
        );
    ''')
    cur.execute("ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS conferente TEXT;")
    cur.execute('''
        CREATE TABLE IF NOT EXISTS cargas (
          id SERIAL PRIMARY KEY, numero_carga TEXT UNIQUE NOT NULL, motorista TEXT, descricao_romaneio TEXT,
          peso_total NUMERIC, entregas INTEGER, data_emissao TEXT, criado_em TIMESTAMP DEFAULT NOW()
        );
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS carga_pedidos (
          id SERIAL PRIMARY KEY, numero_carga TEXT REFERENCES cargas(numero_carga) ON DELETE CASCADE, pedido_numero TEXT
        );
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS carga_grupos (
          id SERIAL PRIMARY KEY, numero_carga TEXT REFERENCES cargas(numero_carga) ON DELETE CASCADE,
          grupo_codigo TEXT, grupo_titulo TEXT, UNIQUE (numero_carga, grupo_codigo)
        );
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS carga_itens (
          id SERIAL PRIMARY KEY, numero_carga TEXT REFERENCES cargas(numero_carga) ON DELETE CASCADE,
          grupo_codigo TEXT, fabricante TEXT, codigo TEXT, cod_barras TEXT, descricao TEXT,
          qtd_unidades INTEGER, unidade TEXT, pack_qtd INTEGER, pack_unid TEXT, observacao TEXT DEFAULT '',
          separado BOOLEAN DEFAULT FALSE, forcar_conferido BOOLEAN DEFAULT FALSE,
          faltou BOOLEAN DEFAULT FALSE, sobrando INTEGER DEFAULT 0
        );
    ''')
    conn.commit()
    cur.close()
    conn.close()

def extrair_dados_do_pdf(stream, nome_da_carga, nome_arquivo):
    try:
        import fitz
        import re
        documento = fitz.open(stream=stream, filetype="pdf")
        produtos_finais = []
        dados_cabecalho = {}
        
        inicio_extracao = False

        for i, pagina in enumerate(documento):
            if i == 0:
                def extrair_campo_regex(pattern, text):
                    match = re.search(pattern, text, re.DOTALL)
                    return match.group(1).replace('\n', ' ').strip() if match else "N/E"

                texto_completo_pagina = pagina.get_text("text")
                numero_pedido = extrair_campo_regex(r"Pedido:\s*(\d+)", texto_completo_pagina)
                if numero_pedido == "N/E":
                    numero_pedido = extrair_campo_regex(r"Pedido\s+(\d+)", texto_completo_pagina)

                nome_cliente = extrair_campo_regex(r"Cliente:\s*(.*?)(?:\s*Cond\. Pgto:|\n)", texto_completo_pagina)

                vendedor = "N/E"
                try:
                    vendedor_rect_list = pagina.search_for("Vendedor")
                    if vendedor_rect_list:
                        vendedor_rect = vendedor_rect_list[0]
                        search_area = fitz.Rect(
                            vendedor_rect.x0 - 15,
                            vendedor_rect.y1,
                            vendedor_rect.x1 + 15,
                            vendedor_rect.y1 + 20
                        )
                        vendedor_words = pagina.get_text("words", clip=search_area)
                        if vendedor_words:
                            vendedor = vendedor_words[0][4]
                except Exception:
                    vendedor = extrair_campo_regex(r"Vendedor\s*([A-ZÀ-Ú]+)", texto_completo_pagina)

                dados_cabecalho = {
                    "numero_pedido": numero_pedido,
                    "nome_cliente": nome_cliente,
                    "vendedor": vendedor
                }

            palavras_na_tabela = pagina.get_text("words")
            if not palavras_na_tabela:
                continue

            palavras_na_tabela.sort(key=lambda p: (p[1], p[0]))

            linhas_agrupadas = []
            linha_atual = [palavras_na_tabela[0]]
            y_referencia = palavras_na_tabela[0][1]
            for j in range(1, len(palavras_na_tabela)):
                palavra = palavras_na_tabela[j]
                if abs(palavra[1] - y_referencia) < 5:
                    linha_atual.append(palavra)
                else:
                    linhas_agrupadas.append(sorted(linha_atual, key=lambda p: p[0]))
                    linha_atual = [palavra]
                    y_referencia = palavra[1]
            linhas_agrupadas.append(sorted(linha_atual, key=lambda p: p[0]))

            for palavras_linha in linhas_agrupadas:
                texto_linha = " ".join([p[4] for p in palavras_linha])
                if "ITEM CÓD. BARRAS" in texto_linha:
                    inicio_extracao = True
                    continue
                elif "**POR GENTILEZA" in texto_linha:
                    inicio_extracao = False
                    continue
                if not inicio_extracao:
                    continue

                product_chunks = []
                current_chunk = []
                if len(palavras_linha) > 1 and palavras_linha[0][4].isdigit() and len(palavras_linha[0][4]) <= 2:
                    current_chunk.append(palavras_linha[0])
                    for k in range(1, len(palavras_linha)):
                        word_info = palavras_linha[k]
                        word_text = word_info[4]
                        is_start_of_new_product = False
                        if (
                            word_text.isdigit()
                            and len(word_text) <= 2
                            and k + 1 < len(palavras_linha)
                            and palavras_linha[k + 1][4].isdigit()
                            and len(palavras_linha[k + 1][4]) > 5
                        ):
                            is_start_of_new_product = True
                        if is_start_of_new_product:
                            product_chunks.append(current_chunk)
                            current_chunk = []
                        current_chunk.append(word_info)
                    product_chunks.append(current_chunk)
                else:
                    product_chunks.append(palavras_linha)

                for chunk in product_chunks:
                    nome_produto_parts = []
                    quantidade_parts = []
                    valores_parts = []

                    for x0, y0, x1, y1, palavra, *_ in chunk:
                        if x0 < 340:
                            nome_produto_parts.append(palavra)
                        elif x0 < 450:
                            quantidade_parts.append(palavra)
                        else:
                            valores_parts.append(palavra)

                    if not nome_produto_parts:
                        continue

                    if (
                        len(nome_produto_parts) > 2
                        and nome_produto_parts[0].isdigit()
                        and len(nome_produto_parts[0]) <= 2
                    ):
                        nome_produto_final = " ".join(nome_produto_parts[1:])
                    else:
                        nome_produto_final = " ".join(nome_produto_parts)

                    quantidade_completa_str = " ".join(quantidade_parts)

                    valor_total_item = "0.00"
                    if valores_parts:
                        match_valor = re.search(r'[\d,.]+', valores_parts[-1])
                        if match_valor:
                            valor_total_item = match_valor.group(0)

                    unidades_pacote = 1
                    match_unidades = re.search(r'C/\s*(\d+)', quantidade_completa_str, re.IGNORECASE)
                    if match_unidades:
                        unidades_pacote = int(match_unidades.group(1))

                    if nome_produto_final and quantidade_completa_str:
                        produtos_finais.append({
                            "produto_nome": nome_produto_final,
                            "quantidade_pedida": quantidade_completa_str,
                            "quantidade_entregue": None,
                            "status": "Pendente",
                            "valor_total_item": valor_total_item.replace(',', '.'),
                            "unidades_pacote": unidades_pacote,
                            "forced_confirmed": False
                        })

        documento.close()

        if not produtos_finais:
            return {"erro": "Nenhum produto pôde ser extraído do PDF."}

        return {
            **dados_cabecalho,
            "produtos": produtos_finais,
            "status_conferencia": "Pendente",
            "nome_da_carga": nome_da_carga,
            "nome_arquivo": nome_arquivo
        }

    except Exception as e:
        import traceback
        return {"erro": f"Erro na extração do PDF: {str(e)}\n{traceback.format_exc()}"}


def salvar_no_banco_de_dados(dados_do_pedido):
    conn = get_db_connection()
    cur = conn.cursor()
    sql = "INSERT INTO pedidos (numero_pedido, nome_cliente, vendedor, nome_da_carga, nome_arquivo, status_conferencia, produtos, url_pdf) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (numero_pedido) DO NOTHING;"
    cur.execute(sql, (dados_do_pedido.get('numero_pedido'), dados_do_pedido.get('nome_cliente'), dados_do_pedido.get('vendedor'), dados_do_pedido.get('nome_da_carga'), dados_do_pedido.get('nome_arquivo'), dados_do_pedido.get('status_conferencia', 'Pendente'), json.dumps(dados_do_pedido.get('produtos', [])), dados_do_pedido.get('url_pdf')))
    conn.commit()
    cur.close()
    conn.close()

init_db()

@app.before_request
def _force_root_home():
    if request.path == '/':
        return render_template('home_apps.html')

@app.route("/")
def pagina_inicial(): return render_template("home_apps.html")
@app.get("/conferencia")
def conferencia_redirect(): return redirect("/conferencia/", code=301)
@app.get("/conferencia/")
def pagina_conferencia(): return render_template("conferencia.html")
@app.route("/gestao")
def pagina_gestao(): return render_template('gestao.html')
@app.route('/conferencia/<nome_da_carga>')
def pagina_lista_pedidos(nome_da_carga): return render_template('lista_pedidos.html', nome_da_carga=nome_da_carga)

@app.route("/pedido/<pedido_id>")
def detalhe_pedido(pedido_id):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM pedidos WHERE numero_pedido = %s;", (pedido_id,))
    pedido_encontrado = cur.fetchone()
    cur.close()
    conn.close()
    if pedido_encontrado:
        return render_template('detalhe_pedido.html', pedido=pedido_encontrado)
    return "Pedido não encontrado", 404

@app.route('/api/upload/<nome_da_carga>', methods=['POST'])
def upload_files(nome_da_carga):
    if 'files[]' not in request.files: return jsonify({"sucesso": False, "erro": "Nenhum arquivo enviado."}), 400
    files = request.files.getlist('files[]')
    erros, sucessos = [], 0
    for file in files:
        if file.filename == '': continue
        filename = secure_filename(file.filename)
        try:
            pdf_bytes = file.read()
            dados_extraidos = extrair_dados_do_pdf(nome_da_carga=nome_da_carga, nome_arquivo=filename, stream=pdf_bytes)
            if "erro" in dados_extraidos:
                erros.append(f"Arquivo '{filename}': {dados_extraidos['erro']}")
                continue
            upload_result = cloudinary.uploader.upload(pdf_bytes, resource_type="raw", public_id=f"pedidos/{filename}")
            dados_extraidos['url_pdf'] = upload_result['secure_url']
            salvar_no_banco_de_dados(dados_extraidos)
            sucessos += 1
        except Exception as e:
            import traceback
            erros.append(f"Arquivo '{filename}': Falha inesperada no processamento. {traceback.format_exc()}")
    if erros: return jsonify({"sucesso": False, "erro": f"{sucessos} arquivo(s) processado(s). ERROS: {'; '.join(erros)}"})
    return jsonify({"sucesso": True, "mensagem": f"Todos os {sucessos} arquivo(s) da carga '{nome_da_carga}' foram processados."})

@app.route('/api/cargas')
def api_cargas():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT nome_da_carga FROM pedidos WHERE nome_da_carga IS NOT NULL ORDER BY nome_da_carga;")
    cargas = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return jsonify(cargas)

@app.route('/api/pedidos/<nome_da_carga>')
def api_pedidos_por_carga(nome_da_carga):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM pedidos WHERE nome_da_carga = %s ORDER BY id DESC;", (nome_da_carga,))
    pedidos = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(pedidos)

@app.route('/api/item/update', methods=['POST'])
def update_item_status():
    dados_recebidos = request.json
    status_final = "Erro"
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM pedidos WHERE numero_pedido = %s;", (dados_recebidos['pedido_id'],))
        pedido = cur.fetchone()
        if not pedido: return jsonify({"sucesso": False, "erro": "Pedido não encontrado."}), 404
        produtos_atualizados = pedido['produtos']
        todos_conferidos = True
        for produto in produtos_atualizados:
            if produto['produto_nome'] == dados_recebidos['produto_nome']:
                qtd_entregue_str = dados_recebidos['quantidade_entregue']
                produto['quantidade_entregue'] = qtd_entregue_str
                produto['observacao'] = dados_recebidos.get('observacao', '')
                if bool(produto.get('forced_confirmed', False)):
                    status_final = "Confirmado"
                    produto['status'] = status_final
                    break
                qtd_pedida_str = produto.get('quantidade_pedida', '0')
                unidades_pacote = int(produto.get('unidades_pacote', 1))
                match_pacotes = re.match(r'(\d+)', qtd_pedida_str)
                pacotes_pedidos = int(match_pacotes.group(1)) if match_pacotes else 0
                total_unidades_pedidas = pacotes_pedidos * unidades_pacote
                try:
                    qtd_entregue_int = int(qtd_entregue_str)
                    if qtd_entregue_int == total_unidades_pedidas: status_final = "Confirmado"
                    elif qtd_entregue_int == 0: status_final = "Corte Total"
                    else: status_final = "Corte Parcial"
                except (ValueError, TypeError): status_final = "Corte Parcial"
                produto['status'] = status_final
                break
        for produto in produtos_atualizados:
            if produto['status'] == 'Pendente':
                todos_conferidos = False
                break
        novo_status_conferencia = 'Finalizado' if todos_conferidos else 'Pendente'
        sql_update = "UPDATE pedidos SET produtos = %s, status_conferencia = %s WHERE numero_pedido = %s;"
        cur.execute(sql_update, (json.dumps(produtos_atualizados), novo_status_conferencia, dados_recebidos['pedido_id']))
        conn.commit()
        return jsonify({"sucesso": True, "status_final": status_final})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"sucesso": False, "erro": str(e)}), 500
    finally:
        if conn: cur.close(); conn.close()

@app.route('/api/item/force', methods=['POST'])
def force_item():
    dados = request.json
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM pedidos WHERE numero_pedido = %s;", (dados['pedido_id'],))
        pedido = cur.fetchone()
        if not pedido: return jsonify({"sucesso": False, "erro": "Pedido não encontrado."}), 404
        produtos = pedido['produtos']
        novo_forced = None
        novo_status = None
        for produto in produtos:
            if produto.get('produto_nome') == dados.get('produto_nome'):
                atual = bool(produto.get('forced_confirmed', False))
                produto['forced_confirmed'] = not atual
                novo_forced = produto['forced_confirmed']
                if produto['forced_confirmed']: produto['status'] = 'Confirmado'
                else: produto['status'] = 'Pendente'
                novo_status = produto['status']
                break
        cur.execute("UPDATE pedidos SET produtos = %s WHERE numero_pedido = %s;", (json.dumps(produtos), dados['pedido_id']))
        conn.commit()
        return jsonify({"sucesso": True, "forced_confirmed": novo_forced, "status": novo_status})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"sucesso": False, "erro": str(e)}), 500
    finally:
        if conn: cur.close(); conn.close()

@app.route('/api/cortes')
def api_cortes():
    cortes_agrupados = defaultdict(list)
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM pedidos WHERE status_conferencia = 'Finalizado';")
        pedidos = cur.fetchall()
        for pedido in pedidos:
            produtos = pedido.get('produtos', []) if pedido.get('produtos') is not None else []
            if not isinstance(produtos, list): continue
            nome_carga = pedido.get('nome_da_carga', 'Sem Carga')
            for produto in produtos:
                if produto.get('status') in ['Corte Parcial', 'Corte Total']:
                    cortes_agrupados[nome_carga].append({
                        "numero_pedido": pedido.get('numero_pedido'), "nome_cliente": pedido.get('nome_cliente'),
                        "vendedor": pedido.get('vendedor'), "observacao": produto.get('observacao', ''),
                        "produto": produto
                    })
        return jsonify(cortes_agrupados)
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"erro": str(e)}), 500
    finally:
        if conn: cur.close(); conn.close()

@app.route('/api/gerar-relatorio')
def gerar_relatorio():
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM pedidos;")
        pedidos = cur.fetchall()
        if not pedidos: return "Nenhum pedido encontrado para gerar o relatório.", 404
        dados_para_excel = []
        for pedido in pedidos:
            produtos = pedido.get('produtos', []) if pedido.get('produtos') is not None else []
            if not isinstance(produtos, list): continue
            for produto in produtos:
                if produto.get('status') in ['Corte Parcial', 'Corte Total']:
                    try:
                        valor_total = float(str(produto.get('valor_total_item', '0')).replace(',', '.'))
                        unidades_pacote = int(produto.get('unidades_pacote', 1))
                        qtd_pedida_str = produto.get('quantidade_pedida', '0')
                        match = re.match(r'(\d+)', qtd_pedida_str)
                        pacotes_pedidos = int(match.group(1)) if match else 0
                        preco_por_pacote = valor_total / pacotes_pedidos if pacotes_pedidos > 0 else 0
                        preco_unidade = preco_por_pacote / unidades_pacote if unidades_pacote > 0 else 0
                        unidades_pedidas = pacotes_pedidos * unidades_pacote
                        qtd_entregue_str = str(produto.get('quantidade_entregue', '0'))
                        unidades_entregues = int(qtd_entregue_str) if qtd_entregue_str.isdigit() else 0
                        valor_corte = (unidades_pedidas - unidades_entregues) * preco_unidade
                        dados_para_excel.append({
                            'Pedido': pedido.get('numero_pedido'), 'Cliente': pedido.get('nome_cliente'),
                            'Vendedor': pedido.get('vendedor'), 'Produto': produto.get('produto_nome', ''),
                            'Quantidade Pedida': produto.get('quantidade_pedida', ''),
                            'Quantidade Entregue': produto.get('quantidade_entregue', ''),
                            'Status': produto.get('status', ''), 'Observação': produto.get('observacao', ''),
                            'Valor Total Item': produto.get('valor_total_item'),
                            'Valor do Corte Estimado': round(valor_corte, 2)
                        })
                    except (ValueError, TypeError, AttributeError) as e:
                        print(f"Erro ao calcular corte para o produto {produto.get('produto_nome', 'N/A')}: {e}")
                        continue
        if not dados_para_excel: return "Nenhum item com corte encontrado para gerar o relatório."
        df = pd.DataFrame(dados_para_excel)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Cortes')
        return Response(
            output.getvalue(),
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment;filename=cortes_relatorio.xlsx"}
        )
    except Exception as e:
        import traceback; traceback.print_exc()
        return f"Erro ao gerar relatório: {e}", 500
    finally:
        if conn: cur.close(); conn.close()

@app.route('/api/resetar-dia', methods=['POST'])
def resetar_dia():
    opts = request.get_json(silent=True) or {}
    limpa_mapas = opts.get("mapas", True)
    limpa_pedidos = opts.get("pedidos", True)
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        if limpa_mapas:
            try:
                cur.execute("TRUNCATE TABLE carga_itens, carga_grupos, carga_pedidos, cargas RESTART IDENTITY CASCADE;")
            except Exception:
                cur.execute("DELETE FROM carga_itens;"); cur.execute("DELETE FROM carga_grupos;")
                cur.execute("DELETE FROM carga_pedidos;"); cur.execute("DELETE FROM cargas;")
        if limpa_pedidos:
            try: cur.execute("TRUNCATE TABLE pedidos RESTART IDENTITY CASCADE;")
            except Exception: cur.execute("DELETE FROM pedidos;")
        conn.commit()
        return jsonify({"sucesso": True, "mensagem": "Dados do dia resetados.", "detalhe": {"mapas": limpa_mapas, "pedidos": limpa_pedidos}})
    except Exception as e:
        if conn: conn.rollback()
        app.logger.exception("Falha ao resetar dia")
        return jsonify({"sucesso": False, "erro": str(e)}), 500
    finally:
        try:
            if cur: cur.close()
        finally:
            if conn: conn.close()
#</editor-fold>

# =================================================================
# ROTAS DO MAPA DE SEPARAÇÃO
# =================================================================

@app.route("/mapa/upload", methods=["POST"])
def mapa_upload():
    file = request.files.get("file")
    if not file:
        flash("Selecione um PDF do mapa.", "warning")
        return redirect(url_for("pagina_gestao"))

    filename = secure_filename(file.filename)
    path_tmp = os.path.join("/tmp", filename)
    file.save(path_tmp)

    try:
        header, _, grupos, itens = parse_mapa(path_tmp)
        numero_carga = header.get("numero_carga")
        if not numero_carga:
            flash("Não foi possível extrair o número da carga do PDF.", "danger")
            return redirect(url_for("pagina_gestao"))

        conn = get_db_connection()
        cur = conn.cursor()

        # Limpa dados antigos da carga para garantir um re-upload limpo
        cur.execute("DELETE FROM cargas WHERE numero_carga = %s;", (numero_carga,))

        # Insere a nova carga
        cur.execute("""
            INSERT INTO cargas (numero_carga, motorista, descricao_romaneio, data_emissao)
            VALUES (%s, %s, %s, %s)
        """, (numero_carga, header.get("motorista"), header.get("romaneio"), header.get("data")))

        # Insere grupos
        for g in grupos or []:
            cur.execute("""
                INSERT INTO carga_grupos (numero_carga, grupo_codigo, grupo_titulo)
                VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
            """, (numero_carga, g.get("grupo_codigo"), g.get("grupo_titulo")))

        # Insere itens
        for it in itens or []:
            cur.execute("""
                INSERT INTO carga_itens (
                    numero_carga, grupo_codigo, fabricante, codigo, cod_barras,
                    descricao, qtd_unidades, unidade, pack_qtd, pack_unid
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                numero_carga, it.get("grupo_codigo"), it.get("fabricante"),
                it.get("codigo"), it.get("cod_barras"), it.get("descricao"),
                it.get("qtd_unidades"), it.get("unidade"),
                it.get("pack_qtd"), it.get("pack_unid")
            ))
        
        conn.commit()
        flash(f"Mapa {numero_carga} importado com sucesso!", "success")
        return redirect(url_for("mapa_detalhe", numero_carga=numero_carga))

    except Exception as e:
        app.logger.error(f"Falha ao processar mapa: {e}", exc_info=True)
        flash(f"Ocorreu um erro ao processar o PDF do mapa: {e}", "danger")
        return redirect(url_for("pagina_gestao"))
    finally:
        cur.close()
        conn.close()
        if os.path.exists(path_tmp):
            os.remove(path_tmp)


@app.route('/mapa/<numero_carga>')
def mapa_detalhe(numero_carga):
    # Esta rota agora simplesmente renderiza o template.
    # Os dados serão carregados via API pelo JavaScript.
    return render_template('mapa_detalhe.html', numero_carga=numero_carga)

@app.route('/mapa')
def mapa_lista():
    # Esta rota renderiza a página que listará os mapas.
    # A lista será preenchida via API.
    return render_template('mapa_lista.html')


@app.route('/api/mapas')
def api_mapas():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT numero_carga, motorista, descricao_romaneio, data_emissao, criado_em FROM cargas ORDER BY criado_em DESC")
    mapas = cur.fetchall()
    cur.close(); conn.close()
    return jsonify(mapas)

@app.route('/api/mapa/<numero_carga>')
def api_mapa_detalhe(numero_carga):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT grupo_codigo, grupo_titulo FROM carga_grupos WHERE numero_carga = %s ORDER BY grupo_codigo;", (numero_carga,))
    grupos = cur.fetchall()
    cur.execute("""
        SELECT id, grupo_codigo, fabricante, codigo, cod_barras, descricao,
               qtd_unidades, unidade, pack_qtd, pack_unid,
               observacao, separado, forcar_conferido, faltou, sobrando
        FROM carga_itens
        WHERE numero_carga = %s
        ORDER BY id;
    """, (numero_carga,))
    itens = cur.fetchall()
    cur.close(); conn.close()
    if not grupos and not itens:
        abort(404, description="Mapa não encontrado ou sem itens.")
    return jsonify({"grupos": grupos, "itens": itens})


@app.route('/api/mapa/item/atualizar', methods=['POST'])
def api_mapa_item_atualizar():
    data = request.json or {}
    item_id = data.get('id')
    if not item_id: return jsonify({"ok": False, "erro": "ID do item é obrigatório"}), 400

    # Pega apenas os campos que podem ser atualizados pelo frontend
    campos_validos = ["separado", "faltou", "forcar_conferido", "observacao", "sobrando"]
    update_fields = {k: v for k, v in data.items() if k in campos_validos}

    if not update_fields: return jsonify({"ok": False, "erro": "Nenhum campo para atualizar"}), 400

    set_clause = ", ".join([f"{key} = %s" for key in update_fields.keys()])
    values = list(update_fields.values()) + [item_id]

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(f"UPDATE carga_itens SET {set_clause} WHERE id = %s", tuple(values))
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"ok": True})


@app.route('/mapa/deletar/<numero_carga>', methods=['POST'])
def mapa_deletar(numero_carga):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM cargas WHERE numero_carga = %s", (numero_carga,))
        conn.commit()
        flash(f"Mapa {numero_carga} excluído com sucesso.", "success")
    except Exception as e:
        conn.rollback()
        app.logger.exception(e)
        flash(f"Erro ao excluir o mapa {numero_carga}.", "danger")
    finally:
        cur.close(); conn.close()
    return redirect(url_for('mapa_lista'))

# =================================================================
# ROTA DE DEBUG AVANÇADA PARA O EXTRATOR
# =================================================================
@app.route('/mapa/extrator', methods=['GET', 'POST'])
def mapa_extrator():
    if request.method == 'GET':
        return '''
        <!doctype html><html lang="pt-br"><head>
        <meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Extrator de Debug</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
        <style>
          body{background:#0f1115;color:#e9edf3}
          .mono{font-family:ui-monospace,Menlo,Consolas,monospace}
          .ok{background:rgba(35,171,103,.14)}
          .fail{background:rgba(239,68,68,.12)}
          table{font-size:.9rem} td,th{vertical-align:top}
          pre{white-space:pre-wrap}
        </style></head><body class="p-3">
        <a class="btn btn-outline-light mb-3" href="/gestao">← Gestão</a>
        <h3>Extrator de Debug do Mapa (PDF)</h3>
        <p class="text-secondary">Envie um PDF para ver as linhas lidas e como cada uma foi interpretada.</p>
        <form method="post" enctype="multipart/form-data" class="d-flex gap-2 mb-4">
          <input class="form-control" type="file" name="pdf" accept="application/pdf" required>
          <button class="btn btn-warning">Processar</button>
        </form>
        </body></html>
        '''

    f = request.files.get('pdf')
    if not f: return "Envie um PDF", 400

    path_tmp = f"/tmp/{secure_filename(f.filename)}"
    f.save(path_tmp)

    # --- LÓGICA DE DEBUG ---
    # Recriamos a lógica do parser aqui para podermos inspecionar passo a passo
    try:
        from typing import List, Tuple
        
        X_FABRICANTE = 430
        X_QUANTIDADE = 500
        Y_LINE_TOLERANCE = 4
        GRUPO_PATTERN = re.compile(r"([A-Z]{2,}\d+-[A-Z0-9/\s]+)")

        def _clean(s: str) -> str:
            if not s: return ""
            return re.sub(r"\s+", " ", s).strip()

        def group_words_into_lines(words: list, y_tolerance: int) -> List[List[Tuple]]:
            if not words: return []
            lines = []
            words.sort(key=lambda w: (w[1], w[0]))
            current_line = [words[0]]
            last_y = words[0][1]
            for i in range(1, len(words)):
                word = words[i]
                y0 = word[1]
                if abs(y0 - last_y) <= y_tolerance:
                    current_line.append(word)
                else:
                    lines.append(sorted(current_line, key=lambda w: w[0]))
                    current_line = [word]
                last_y = y0
            lines.append(sorted(current_line, key=lambda w: w[0]))
            return lines

        doc = fitz.open(path_tmp)
        debug_rows = []
        for page in doc:
            words = page.get_text("words")
            visual_lines = group_words_into_lines(words, Y_LINE_TOLERANCE)
            
            for line_words in visual_lines:
                full_line_text = " ".join(w[4] for w in line_words)
                parsed_data = {}

                desc_parts, fab_parts, qtd_parts = [], [], []
                for x0, y0, x1, y1, text, *_ in line_words:
                    if x0 < X_FABRICANTE: desc_parts.append(text)
                    elif x0 < X_QUANTIDADE: fab_parts.append(text)
                    else: qtd_parts.append(text)
                
                parsed_data['descricao'] = _clean(" ".join(desc_parts))
                parsed_data['fabricante'] = _clean(" ".join(fab_parts))
                parsed_data['quantidade'] = _clean(" ".join(qtd_parts))

                debug_rows.append({"line": full_line_text, "parsed": parsed_data})

    except Exception as e:
        return (f"Erro no extrator de debug: {e}", 400)

    # --- RENDERIZAÇÃO DA TABELA DE DEBUG ---
    head = '''
    <!doctype html><html lang="pt-br"><head>
    <meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Resultado do Extrator</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
      body{background:#0f1115;color:#e9edf3} .mono{font-family:ui-monospace,Menlo,Consolas,monospace}
      .ok{background:rgba(35,171,103,.14)} .fail{background:rgba(239,68,68,.12)}
      table{font-size:.9rem} td,th{vertical-align:top} pre{white-space:pre-wrap}
    </style></head><body class="p-3">
    <a class="btn btn-outline-light mb-3" href="/mapa/extrator">← Novo arquivo</a>
    <h4 class="mb-3">Depuração Linha a Linha</h4>
    <div class="table-responsive"><table class="table table-sm table-dark table-striped align-middle">
      <thead><tr>
        <th>Linha Crua</th><th>Coluna Descrição (Inferida)</th><th>Coluna Fabricante (Inferida)</th><th>Coluna Quantidade (Inferida)</th>
      </tr></thead><tbody>'''
    rows_html = []
    for r in debug_rows:
        p = r["parsed"] or {}
        cls = "ok" if p.get('descricao') or p.get('fabricante') or p.get('quantidade') else "fail"
        rows_html.append(f"<tr class='{cls}'>"
                         f"<td class='mono'><pre>{r['line']}</pre></td>"
                         f"<td class='mono'>{p.get('descricao', '')}</td>"
                         f"<td class='mono'>{p.get('fabricante', '')}</td>"
                         f"<td class='mono'>{p.get('quantidade', '')}</td>"
                         f"</tr>")
    tail = "</tbody></table></div></body></html>"
    return head + "\n".join(rows_html) + tail