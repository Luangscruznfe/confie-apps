# -*- coding: utf-8 -*-
import re
from typing import Dict, List, Tuple, Any

try:
    import fitz  # PyMuPDF
except ImportError:
    raise RuntimeError("PyMuPDF (fitz) não encontrado. Instale com: pip install pymupdf")

# ---------- utils ----------
QTD_UNIDS_TOKEN = r"(UN|FD|CX|CJ|DP|PC|PT|DZ|SC|KT|JG|BF|PA)"

def _clean(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\x0c", " ").replace("\u00ad", "")
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()

def _iter_lines(doc: "fitz.Document"):
    """Gera linhas em ordem de leitura (y, depois x) para TODAS as páginas."""
    for p in range(doc.page_count):
        page = doc.load_page(p)
        blocks = page.get_text("blocks") or []
        blocks.sort(key=lambda b: (round(b[1], 2), round(b[0], 2)))
        for b in blocks:
            txt = b[4] if len(b) > 4 else ""
            for raw in (txt.splitlines() if txt else []):
                line = _clean(raw)
                if line:
                    yield line

# ---------- padrões ----------
GRUPO_RE = re.compile(r"^\s*([A-Z0-9]{3,})\s*-\s*(.+?)\s*$")             # ex.: GBA1 - BALAS/GOMAS
EAN_RE   = re.compile(r"^\d{12,14}$")                                     # 12-14 dígitos
COD_RE   = re.compile(r"^\d{3,}$")                                        # código numérico (3+)
QTD_RE   = re.compile(rf"^(\d+)\s*{QTD_UNIDS_TOKEN}$", re.IGNORECASE)     # "3 UN", "1 DP", ...
PACK_RE  = re.compile(r"^C\s*/\s*(\d+)\s*UN$", re.IGNORECASE)             # "C/ 12UN"
# fabricante: linha curta, toda maiúscula, sem números (ex.: RICLAN, DORI, HARCCLIN)
FAB_RE   = re.compile(r"^[A-ZÀ-ÖØ-Þ]{2,}(?:\s+[A-ZÀ-ÖØ-Þ]{2,})*$")

# ---------- principal ----------
def parse_mapa(pdf_path: str) -> Tuple[Dict[str, str], Any, List[Dict[str, str]], List[Dict[str, Any]]]:
    """
    Retorna: header, None, grupos, itens

    grupos: [{"grupo_codigo": "...", "grupo_titulo": "..."}]
    itens:  [{"grupo_codigo": "...", "fabricante": "...", "codigo": "...", "cod_barras": "...",
              "descricao": "...", "qtd_unidades": int, "unidade": "UN",
              "pack_qtd": int, "pack_unid": "UN"}]
    """
    doc = fitz.open(pdf_path)

    header: Dict[str, str] = {}
    grupos: List[Dict[str, str]] = []
    itens:  List[Dict[str, Any]] = []

    grupo_codigo = ""
    grupo_titulo = ""

    # estado do item em construção (FSM)
    cur: Dict[str, Any] = {}
    esperando = "fabricante"  # fabricante -> codigo -> ean -> descricao -> qtd -> (pack opcional)

    def flush_item():
        """Encerra o item atual se houver descrição; seta defaults e empilha."""
        nonlocal cur, esperando
        if not cur.get("descricao"):
            cur = {}
            esperando = "fabricante"
            return
        cur.setdefault("qtd_unidades", 0)
        cur.setdefault("unidade", "UN")
        cur.setdefault("pack_qtd", 1)
        cur.setdefault("pack_unid", "UN")
        cur["grupo_codigo"] = grupo_codigo or cur.get("grupo_codigo") or ""
        itens.append(cur)
        cur = {}
        esperando = "fabricante"

    for line in _iter_lines(doc):
        # 1) Grupo?
        mg = GRUPO_RE.match(line)
        if mg:
            if cur:
                flush_item()
            grupo_codigo, grupo_titulo = mg.group(1).strip(), _clean(mg.group(2))
            grupos.append({"grupo_codigo": grupo_codigo, "grupo_titulo": grupo_titulo})
            continue

        # 2) Pack "C/ 12UN" pode vir após a quantidade
        mpk = PACK_RE.match(line)
        if mpk and cur:
            cur["pack_qtd"]  = int(mpk.group(1))
            cur["pack_unid"] = "UN"
            continue

        # 3) FSM do item
        if esperando == "fabricante":
            # muitos mapas trazem um número de sequência (ex.: "22") sozinho — ignorar
            if line.isdigit():
                continue
            if FAB_RE.match(line) and len(line) <= 30:
                cur = {"fabricante": line}
                esperando = "codigo"
                continue
            if COD_RE.match(line):  # sem fabricante
                cur = {"codigo": line}
                esperando = "ean"
                continue
            if EAN_RE.match(line):  # raríssimo
                cur = {"cod_barras": line}
                esperando = "descricao"
                continue
            if len(line) > 3:       # fallback vira descrição
                cur = {"descricao": line}
                esperando = "qtd"
                continue

        elif esperando == "codigo":
            if COD_RE.match(line):
                cur["codigo"] = line
                esperando = "ean"
                continue
            if FAB_RE.match(line):  # fabricante repetido
                cur["fabricante"] = line
                continue
            if len(line) > 3 and not line.isdigit():  # descrição antes do EAN
                cur["descricao"] = line
                esperando = "qtd"
                continue

        elif esperando == "ean":
            if EAN_RE.match(line):
                cur["cod_barras"] = line
                esperando = "descricao"
                continue
            if len(line) > 3 and not QTD_RE.match(line):  # sem EAN
                cur["descricao"] = line
                esperando = "qtd"
                continue

        elif esperando == "descricao":
            mq = QTD_RE.match(line)
            if mq:
                cur["qtd_unidades"] = int(mq.group(1))
                cur["unidade"] = mq.group(2).upper()
                flush_item()
                continue
            # descrição pode quebrar em 2+ linhas
            desc = cur.get("descricao", "")
            cur["descricao"] = (desc + " " + line).strip() if desc else line
            continue

        elif esperando == "qtd":
            mq = QTD_RE.match(line)
            if mq:
                cur["qtd_unidades"] = int(mq.group(1))
                cur["unidade"] = mq.group(2).upper()
                flush_item()
                continue
            # não reconheceu qtd? pode ser início de novo item; fecha o atual
            if GRUPO_RE.match(line) or FAB_RE.match(line) or COD_RE.match(line) or EAN_RE.match(line):
                flush_item()
                # reprocessa indiretamente (o loop já vai tratar essa linha)
                if FAB_RE.match(line):
                    cur = {"fabricante": line}; esperando = "codigo"
                elif COD_RE.match(line):
                    cur = {"codigo": line}; esperando = "ean"
                elif EAN_RE.match(line):
                    cur = {"cod_barras": line}; esperando = "descricao"
                continue
            # se nada casa, anexa à descrição
            cur["descricao"] = (cur.get("descricao", "") + " " + line).strip()

    # flush do último item
    if cur:
        flush_item()

    # (Opcional) tentar header["numero_carga"] aqui, se o PDF trouxer isso
    # Por enquanto deixamos vazio e usamos o que já vem de fora (rota).

    return header, None, grupos, itens

# ---------- depuração ----------
def debug_extrator(pdf_path: str):
    """
    Retorna linhas + tentativa de interpretação parcial (grupo/cód/EAN/quantidade).
    Útil para inspecionar rapidamente o que o parser está vendo em /mapa/extrator.
    """
    doc = fitz.open(pdf_path)
    rows = []
    n = 0
    for line in _iter_lines(doc):
        n += 1
        parsed = {}
        mg = GRUPO_RE.match(line)
        if mg:
            parsed = {"grupo_codigo": mg.group(1), "grupo_titulo": mg.group(2)}
        else:
            if COD_RE.match(line):
                parsed = {"codigo": line}
            elif EAN_RE.match(line):
                parsed = {"cod_barras": line}
            elif QTD_RE.match(line):
                m = QTD_RE.match(line)
                parsed = {"qtd_unidades": int(m.group(1)), "unidade": m.group(2).upper()}
        rows.append({"n": n, "line": line, "parsed": parsed})
    doc.close()
    return rows
