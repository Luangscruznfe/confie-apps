# -*- coding: utf-8 -*-
import re
from typing import Dict, List, Tuple, Any

try:
    import fitz  # PyMuPDF
except ImportError:
    raise RuntimeError("PyMuPDF (fitz) não encontrado. Instale com: pip install pymupdf")

# ===== Constantes de Layout (Ajuste se o PDF mudar) =====
X_FABRICANTE = 430
X_QUANTIDADE = 500
Y_LINE_TOLERANCE = 4  # Tolerância vertical para agrupar palavras na mesma linha

# Regex para identificar um padrão de grupo (ex: "GBA1-...")
GRUPO_PATTERN = re.compile(r"([A-Z]{2,}\d+-[A-Z0-9/\s]+)")


# ---------- utils ----------
def _clean(s: str) -> str:
    if not s: return ""
    return re.sub(r"\s+", " ", s).strip()

def group_words_into_lines(words: list, y_tolerance: int) -> List[List[Tuple]]:
    """
    Agrupa palavras em 'linhas visuais' com uma tolerância vertical.
    Isso corrige o problema de fabricantes que aparecem um pouco abaixo da linha.
    """
    if not words: return []
    
    lines = []
    words.sort(key=lambda w: (w[1], w[0])) # Ordena por Y, depois X
    
    current_line = [words[0]]
    last_y = words[0][1]

    for i in range(1, len(words)):
        word = words[i]
        y0 = word[1]
        
        if abs(y0 - last_y) <= y_tolerance:
            current_line.append(word)
        else:
            lines.append(sorted(current_line, key=lambda w: w[0])) # Ordena a linha por X
            current_line = [word]
        last_y = y0
        
    lines.append(sorted(current_line, key=lambda w: w[0]))
    return lines

# ===== PARSER PRINCIPAL (VERSÃO FINAL) =====

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
            if "Cód. Barras" in full_line_text or "Código Descrição" in full_line_text:
                continue

            # Detecção e extração de grupo (mesmo que esteja 'colado' no texto)
            match_grupo = GRUPO_PATTERN.search(full_line_text)
            if match_grupo:
                grupo_str = _clean(match_grupo.group(1))
                partes_grupo = [p.strip() for p in grupo_str.split('-', 1)]
                if len(partes_grupo) == 2:
                    grupo_codigo_atual = partes_grupo[0]
                    if not any(g['grupo_codigo'] == grupo_codigo_atual for g in grupos):
                        grupos.append({"grupo_codigo": grupo_codigo_atual, "grupo_titulo": partes_grupo[1]})
                
                # Remove o texto do grupo da linha para processar o resto como item
                full_line_text = GRUPO_PATTERN.sub('', full_line_text).strip()
                # Recria as palavras da linha sem o texto do grupo
                temp_words = []
                for w in line_words:
                    if w[4] not in grupo_str:
                        temp_words.append(w)
                line_words = temp_words


            if not line_words or not full_line_text:
                continue
            
            # Divide as palavras restantes em colunas
            desc_parts, fab_parts, qtd_parts = [], [], []
            for x0, y0, x1, y1, text, *_ in line_words:
                if x0 < X_FABRICANTE: desc_parts.append(text)
                elif x0 < X_QUANTIDADE: fab_parts.append(text)
                else: qtd_parts.append(text)

            full_desc = _clean(" ".join(desc_parts))
            fabricante = _clean(" ".join(fab_parts))
            quantidade = _clean(" ".join(qtd_parts))

            if not full_desc: continue
            
            # Processa a descrição para extrair EAN, código e nome
            item = {"grupo_codigo": grupo_codigo_atual, "fabricante": fabricante, "quantidade_str": quantidade}
            desc_words = full_desc.split()
            if desc_words:
                if re.match(r"^\d{12,14}$", desc_words[0]):
                    item["cod_barras"] = desc_words.pop(0)
                
                if desc_words and re.match(r"^\d{3,}$", desc_words[0]):
                    item["codigo"] = desc_words.pop(0)

            item["descricao"] = " ".join(desc_words)
            
            # Extrai unidades e pack da string de quantidade
            match_unidade = re.match(r'(\d+)\s*([A-Z]+)', quantidade);
            if match_unidade:
                item["qtd_unidades"] = int(match_unidade.group(1))
                item["unidade"] = match_unidade.group(2).upper()
            
            match_pack = re.search(r'C/\s*(\d+)', quantidade, re.I)
            if match_pack: item["pack_qtd"] = int(match_pack.group(1))

            if item.get("descricao") or item.get("codigo"):
                itens.append(item)

    doc.close()
    return header, None, grupos, itens


# Função de depuração
def debug_extrator(pdf_path: str):
    doc = fitz.open(pdf_path)
    rows = []
    for page in doc: rows.extend(page.get_text("text").splitlines())
    doc.close()
    return [{"n": i+1, "line": line, "parsed": {}} for i, line in enumerate(rows)]