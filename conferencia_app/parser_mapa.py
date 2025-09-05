# Arquivo: parser_mapa.py (Versão com suporte a itens sem grupo)

import re
from typing import Dict, List, Tuple, Any

try:
    import fitz  # PyMuPDF
except ImportError:
    raise RuntimeError("PyMuPDF (fitz) não encontrado. Instale com: pip install pymupdf")

# ===== Constantes de Layout e Padrões =====
X_FABRICANTE = 430
X_QUANTIDADE = 500
Y_LINE_TOLERANCE = 4
GRUPO_CODE_PATTERN = re.compile(r"([A-Z]{2,}\d{1,2})")
HEADER_KEYWORDS = ["Data Emissão", "PAG.:", "SEPARAÇÃO DE CARGA", "Peso Total", "Pedidos:", "AtivLogRomSepa"]

# ---------- Funções Auxiliares ----------
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

# ===== PARSER PRINCIPAL =====
def parse_mapa(pdf_path: str) -> Tuple[Dict[str, str], Any, List[Dict[str, str]], List[Dict[str, Any]]]:
    doc = fitz.open(pdf_path)
    header, itens = {}, []
    
    # ===== NOVA LÓGICA AQUI =====
    # Define um grupo padrão para itens "avulsos" que vêm antes do primeiro grupo
    grupo_codigo_atual = "GERAL"
    grupos = [{"grupo_codigo": "GERAL", "grupo_titulo": "ITENS SEM GRUPO"}]
    # ============================

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
            
            if any(keyword in full_line_text for keyword in HEADER_KEYWORDS):
                continue

            if not full_line_text or "Cód. Barras" in full_line_text:
                continue
            
            desc_parts, fab_parts, qtd_parts = [], [], []
            for x0, _, _, _, text, *_ in line_words:
                if x0 < X_FABRICANTE: desc_parts.append(text)
                elif x0 < X_QUANTIDADE: fab_parts.append(text)
                else: qtd_parts.append(text)

            full_desc = _clean(" ".join(desc_parts))
            fabricante = _clean(" ".join(fab_parts))
            quantidade = _clean(" ".join(qtd_parts))

            match_grupo_code = GRUPO_CODE_PATTERN.match(full_desc)
            is_group_line = (match_grupo_code and not fabricante and not quantidade)

            if not quantidade and not is_group_line:
                continue

            is_item_with_group = (match_grupo_code and (fabricante or quantidade))

            if is_group_line or is_item_with_group:
                grupo_codigo_atual = match_grupo_code.group(1)
                temp_desc = GRUPO_CODE_PATTERN.sub('', full_desc).strip()
                titulo_grupo = re.sub(r"^-?\s*", "", temp_desc)

                if not any(g['grupo_codigo'] == grupo_codigo_atual for g in grupos):
                    grupos.append({"grupo_codigo": grupo_codigo_atual, "grupo_titulo": titulo_grupo})
                
                if is_group_line:
                    continue
                
                full_desc = titulo_grupo
            
            if not full_desc: continue
            
            item = {"grupo_codigo": grupo_codigo_atual, "fabricante": fabricante}
            
            desc_words = full_desc.split()
            if desc_words:
                if re.match(r"^\d{12,14}$", desc_words[0]):
                    item["cod_barras"] = desc_words.pop(0)
                
                if desc_words and re.match(r"^\d{3,}$", desc_words[0]):
                    item["codigo"] = desc_words.pop(0)

            item["descricao"] = " ".join(desc_words)
            
            item["qtd_unidades"] = 0; item["unidade"] = "UN"
            match_unidade = re.match(r'(\d+)\s*([A-Z]+)', quantidade);
            if match_unidade:
                item["qtd_unidades"] = int(match_unidade.group(1))
                item["unidade"] = match_unidade.group(2).upper()
            
            item["pack_qtd"] = 1; item["pack_unid"] = "UN"
            match_pack = re.search(r'C/\s*(\d+)', quantidade, re.I)
            if match_pack: item["pack_qtd"] = int(match_pack.group(1))

            if item.get("descricao") or item.get("codigo"):
                itens.append(item)

    doc.close()
    return header, None, grupos, itens


def debug_extrator(pdf_path: str):
    doc = fitz.open(pdf_path)
    rows = []
    for page in doc:
        rows.extend(page.get_text("text").splitlines())
    doc.close()
    return [{"n": i + 1, "line": line, "parsed": {}} for i, line in enumerate(rows)]