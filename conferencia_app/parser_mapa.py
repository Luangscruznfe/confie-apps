# -*- coding: utf-8 -*-
import re
from typing import Dict, List, Tuple, Any

try:
    import fitz  # PyMuPDF
except ImportError:
    raise RuntimeError("PyMuPDF (fitz) não encontrado. Instale com: pip install pymupdf")

# ===== NOVAS REGRAS DE EXTRAÇÃO (REGEX) =====

# Regex para o cabeçalho do grupo (ex: "GBA1 - BALAS/GOMAS")
# Tornada mais flexível para encontrar o padrão em qualquer lugar da linha.
GRUPO_RE = re.compile(r"([A-Z0-9]{3,}\s*-\s*.+)")

# Regex para identificar a parte da quantidade no final da linha de um item.
# Ex: "1 UN", "2 DP C/30UN", "1 CX C/20UN"
QTD_RE = re.compile(r"(\d+\s+(?:UN|FD|CX|CJ|DP|PC|PT|DZ|SC|KT|JG|BF|PA)\s*(?:C\/\s*\d+UN)?)", re.IGNORECASE)

# Regex para identificar o código de barras (EAN) no início da linha.
EAN_RE = re.compile(r"^\d{12,14}")

# Regex para identificar o código interno do produto (geralmente após o EAN).
COD_RE = re.compile(r"^\d{3,}")

# ---------- utils ----------

def _clean(s: str) -> str:
    """Limpa a string de caracteres indesejados e espaços múltiplos."""
    if not s:
        return ""
    s = s.replace("\x0c", " ").replace("\u00ad", "")
    return re.sub(r"\s+", " ", s).strip()

def _iter_lines(doc: "fitz.Document"):
    """Gera linhas em ordem de leitura (y, depois x) para TODAS as páginas."""
    for p in range(doc.page_count):
        page = doc.load_page(p)
        # Usar 'blocks' com sort=True é uma boa maneira de obter a ordem de leitura.
        blocks = page.get_text("blocks", sort=True) or []
        for b in blocks:
            # O bloco 4 contém o texto
            txt = b[4] if len(b) > 4 else ""
            for raw in (txt.splitlines() if txt else []):
                line = _clean(raw)
                # Ignora linhas que são cabeçalhos de tabela repetidos
                if line and "Cód. Barras" not in line and "Código Descrição" not in line:
                    yield line

# ===== PARSER PRINCIPAL (LÓGICA REESCRITA) =====

def parse_mapa(pdf_path: str) -> Tuple[Dict[str, str], Any, List[Dict[str, str]], List[Dict[str, Any]]]:
    """
    Nova versão do parser, adaptada para o layout colunar do arquivo mk.pdf.
    """
    doc = fitz.open(pdf_path)
    lines = list(_iter_lines(doc))
    header_text = "\n".join(lines[:30]) # Primeiras linhas para cabeçalho

    header: Dict[str, str] = {}
    m = re.search(r"N[uú]mero da Carga:\s*(\d+)", header_text, re.IGNORECASE)
    if m: header["numero_carga"] = m.group(1).strip()

    m = re.search(r"Data Emiss[aã]o:\s*([0-3]?\d\/[01]?\d\/\d{2,4})", header_text, re.IGNORECASE)
    if m: header["data"] = m.group(1).strip()

    m = re.search(r"Motorista:\s*(.+)", header_text, re.IGNORECASE)
    if m: header["motorista"] = m.group(1).strip()

    m = re.search(r"Desc\.?\s*Romaneio:\s*([A-Z0-9 \-\/]+)", header_text, re.IGNORECASE)
    if m: header["romaneio"] = m.group(1).strip()


    grupos: List[Dict[str, str]] = []
    itens: List[Dict[str, Any]] = []
    grupo_codigo_atual = ""

    for line in lines:
        # 1. Tenta identificar se a linha é (ou contém) um grupo
        match_grupo = GRUPO_RE.search(line)
        if match_grupo:
            texto_grupo = match_grupo.group(1).strip()
            # Divide o código da descrição (ex: "GBA1 - BALAS/GOMAS")
            partes_grupo = [p.strip() for p in texto_grupo.split('-', 1)]
            if len(partes_grupo) == 2:
                grupo_codigo_atual = partes_grupo[0]
                grupos.append({"grupo_codigo": grupo_codigo_atual, "grupo_titulo": partes_grupo[1]})
                # Remove a informação do grupo da linha para que o resto possa ser processado como item
                line = GRUPO_RE.sub('', line).strip()

        # 2. Se sobrou texto na linha, tenta processá-lo como um item
        if not line:
            continue
            
        # 3. Disseca a linha do item (lógica principal)
        # A estratégia é extrair as partes conhecidas (como quantidade e fabricante)
        # e o que sobra é a descrição/código.
        
        item = {"grupo_codigo": grupo_codigo_atual}
        
        # Extrai a quantidade do final da linha
        match_qtd = QTD_RE.search(line)
        if match_qtd:
            qtd_str = match_qtd.group(1)
            item["quantidade_str"] = qtd_str # Armazena a string completa da qtd
            line = line.replace(qtd_str, "").strip() # Remove da linha

            # Tenta extrair o "pack" (C/ 12UN)
            match_pack = re.search(r'C\/\s*(\d+)', qtd_str, re.IGNORECASE)
            if match_pack:
                item["pack_qtd"] = int(match_pack.group(1))

            # Extrai a unidade principal (UN, FD, CX, etc.)
            match_unidade = re.match(r'(\d+)\s*([A-Z]+)', qtd_str)
            if match_unidade:
                item["qtd_unidades"] = int(match_unidade.group(1))
                item["unidade"] = match_unidade.group(2).upper()

        # O que sobrou na linha são EAN, Código, Descrição e Fabricante
        # O fabricante é a última palavra (ou conjunto de palavras em maiúsculo)
        partes = line.split()
        if len(partes) > 1 and partes[-1].isupper():
            item["fabricante"] = partes[-1]
            line = " ".join(partes[:-1]).strip()

        # Agora, processa o início da linha para EAN e Código
        match_ean = EAN_RE.match(line)
        if match_ean:
            item["cod_barras"] = match_ean.group(0)
            line = line.replace(item["cod_barras"], "").strip()
        
        match_cod = COD_RE.match(line)
        if match_cod:
            item["codigo"] = match_cod.group(0)
            line = line.replace(item["codigo"], "").strip()

        # O que finalmente restou é a descrição
        item["descricao"] = line.strip()

        # Adiciona o item à lista apenas se ele tiver uma descrição, para evitar itens vazios
        if item.get("descricao"):
            itens.append(item)

    return header, None, grupos, itens


# ---------- Função de depuração (mantida para testes futuros) ----------
def debug_extrator(pdf_path: str):
    """
    Retorna linhas + tentativa de interpretação parcial.
    Útil para inspecionar rapidamente o que o parser está vendo.
    """
    doc = fitz.open(pdf_path)
    rows = []
    n = 0
    for line in _iter_lines(doc):
        n += 1
        rows.append({"n": n, "line": line, "parsed": {}})
    doc.close()
    return rows