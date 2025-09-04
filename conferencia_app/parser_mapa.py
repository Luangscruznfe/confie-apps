# -*- coding: utf-8 -*-
import re
from typing import Dict, List, Tuple, Any

try:
    import fitz  # PyMuPDF
except ImportError:
    raise RuntimeError("PyMuPDF (fitz) não encontrado. Instale com: pip install pymupdf")

# ===== Constantes de Layout e Padrões (Versão Final) =====
X_FABRICANTE = 430
X_QUANTIDADE = 500
Y_LINE_TOLERANCE = 4

# Padrão flexível para encontrar o código de um grupo (ex: GAA9, GBA1, GCA1)
GRUPO_CODE_PATTERN = re.compile(r"([A-Z]{2,}\d{1,2})")

# ---------- Funções Auxiliares ----------
def _clean(s: str) -> str:
    if not s: return ""
    return re.sub(r"\s+", " ", s).strip()

def group_words_into_lines(words: list, y_tolerance: int) -> List[List[Tuple]]:
    if not words: return []
    lines = []
    # Ordena primariamente por Y, depois por X
    words.sort(key=lambda w: (w[1], w[0]))
    
    current_line = [words[0]]
    last_y = words[0][1]

    for i in range(1, len(words)):
        word = words[i]
        y0 = word[1]
        # Agrupa palavras se a diferença vertical for pequena
        if abs(y0 - last_y) <= y_tolerance:
            current_line.append(word)
        else:
            lines.append(sorted(current_line, key=lambda w: w[0]))
            current_line = [word]
        # Atualiza a referência Y com a do primeiro item da linha para maior estabilidade
        last_y = current_line[0][1]
        
    lines.append(sorted(current_line, key=lambda w: w[0]))
    return lines

# ===== PARSER PRINCIPAL (VERSÃO FINAL E CORRIGIDA) =====

def parse_mapa(pdf_path: str) -> Tuple[Dict[str, str], Any, List[Dict[str, str]], List[Dict[str, Any]]]:
    doc = fitz.open(pdf_path)
    header, grupos, itens = {}, [], []
    grupo_codigo_atual = ""

    for page_num, page in enumerate(doc):
        if page_num == 0:
            text = page.get_text("text")
            m = re.search(r"N[uú]mero da Carga:\s*(\d+)", text, re.I); header["numero_carga"] = m.group(1).strip() if m else ""
            m = re.search(r"Data Emiss[aã]o:\s*([\d/]+)", text, re.I); header["data"] = m.group(1).strip() if m else ""
            m = re.search(r"Motorista:\s*(.+)", text, re.I); header["motorista"] = _clean(m.group(1)) if m else ""
            m = re.search(r"Desc\.?\s*Romaneio:\s*(.+)", text, re.I); header["romaneio"] = _clean(m.group(1)) if m else ""

        words = page.get_text("words")
        visual_lines = group_words_into_lines(words, Y_LINE_TOLERANCE)

        for line_words in visual_lines:
            full_line_text = " ".join(w[4] for w in line_words)
            if not full_line_text or "Cód. Barras" in full_line_text:
                continue
            
            # Divide as palavras da linha em colunas
            desc_parts, fab_parts, qtd_parts = [], [], []
            for x0, _, _, _, text, *_ in line_words:
                if x0 < X_FABRICANTE: desc_parts.append(text)
                elif x0 < X_QUANTIDADE: fab_parts.append(text)
                else: qtd_parts.append(text)

            full_desc = _clean(" ".join(desc_parts))
            fabricante = _clean(" ".join(fab_parts))
            quantidade = _clean(" ".join(qtd_parts))

            # Lógica de detecção de grupo
            # Um grupo pode ser uma linha inteira ou estar no início da descrição de um item
            match_grupo_code = GRUPO_CODE_PATTERN.match(full_desc)
            is_group_line = (match_grupo_code and not fabricante and not quantidade)
            is_item_with_group = (match_grupo_code and (fabricante or quantidade))

            if is_group_line or is_item_with_group:
                grupo_codigo_atual = match_grupo_code.group(1)
                # Remove o código do grupo da descrição
                temp_desc = GRUPO_CODE_PATTERN.sub('', full_desc).strip()
                # O que sobra é o título do grupo, limpando hífens e espaços
                titulo_grupo = re.sub(r"^-?\s*", "", temp_desc)

                if not any(g['grupo_codigo'] == grupo_codigo_atual for g in grupos):
                    grupos.append({"grupo_codigo": grupo_codigo_atual, "grupo_titulo": titulo_grupo})
                
                if is_group_line:
                    continue # Se a linha é SÓ um grupo, pula para a próxima
                
                full_desc = titulo_grupo # O resto da descrição é o início do item
            
            if not full_desc: continue
            
            # Processa o item
            item = {"grupo_codigo": grupo_codigo_atual, "fabricante": fabricante}
            
            desc_words = full_desc.split()
            if desc_words:
                if re.match(r"^\d{12,14}$", desc_words[0]):
                    item["cod_barras"] = desc_words.pop(0)
                
                if desc_words and re.match(r"^\d{3,}$", desc_words[0]):
                    item["codigo"] = desc_words.pop(0)

            item["descricao"] = " ".join(desc_words)
            
            item["qtd_unidades"] = 0
            item["unidade"] = "UN"
            match_unidade = re.match(r'(\d+)\s*([A-Z]+)', quantidade);
            if match_unidade:
                item["qtd_unidades"] = int(match_unidade.group(1))
                item["unidade"] = match_unidade.group(2).upper()
            
            item["pack_qtd"] = 1
            item["pack_unid"] = "UN"
            match_pack = re.search(r'C/\s*(\d+)', quantidade, re.I)
            if match_pack: item["pack_qtd"] = int(match_pack.group(1))

            if item.get("descricao") or item.get("codigo"):
                itens.append(item)

    doc.close()
    return header, None, grupos, itens