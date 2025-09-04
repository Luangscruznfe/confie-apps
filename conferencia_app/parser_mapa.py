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
    for p in range(doc.page_count):
        page = doc.load_page(p)
        blocks = page.get_text("blocks") or []
        blocks.sort(key=lambda b: (round(b[1],2), round(b[0],2)))
        for b in blocks:
            txt = b[4] if len(b) > 4 else ""
            for raw in (txt.splitlines() if txt else []):
                line = _clean(raw)
                if line:
                    yield line

# ---------- padrões ----------
GRUPO_RE = re.compile(r"^\s*([A-Z0-9]{3,})\s*-\s*(.+?)\s*$")
EAN_RE   = re.compile(r"^\d{12,14}$")                         # 12-14 dígitos
COD_RE   = re.compile(r"^\d{3,}$")                            # código numérico (3+)
QTD_RE   = re.compile(rf"^(\d+)\s*{QTD_UNIDS_TOKEN}$", re.IGNORECASE)  # "3 UN", "1 DP", etc.
PACK_RE  = re.compile(r"^C\s*/\s*(\d+)\s*UN$", re.IGNORECASE)          # "C/ 12UN"
# fabricante: linha curta, toda maiúscula, sem números (ex.: RICLAN, DORI, HARCCLIN)
FAB_RE   = re.compile(r"^[A-ZÀ-ÖØ-Þ]{2,}(?:\s+[A-ZÀ-ÖØ-Þ]{2,})*$")

# ---------- principal ----------
def parse_mapa(pdf_path: str) -> Tuple[Dict[str,str], Any, List[Dict[str,str]], List[Dict[str,Any]]]:
    """
    Retorna: header, None, grupos, itens
    grupos: [{"grupo_codigo": "...", "grupo_titulo": "..."}]
    itens:  [{"grupo_codigo": "...", "fabricante": "...", "codigo": "...", "cod_barras": "...",
              "descricao": "...", "qtd_unidades": int, "unidade": "UN",
              "pack_qtd": int, "pack_unid": "UN"}]
    """
    doc = fitz.open(pdf_path)

    header: Dict[str,str] = {}
    grupos: List[Dict[str,str]] = []
    itens:  List[Dict[str,Any]] = []

    grupo_codigo = ""
    grupo_titulo = ""

    # estado do item em construção
    cur: Dict[str, Any] = {}
    esperando = "fabricante"  # fabricante -> codigo -> ean -> descricao -> qtd -> (pack opcional)

    def flush_item():
        nonlocal cur, esperando
        if not cur.get("descricao"):
            cur = {}
            esperando = "fabricante"
            return
        # defaults
        cur.setdefault("qtd_unidades", 0)
        cur.setdefault("unidade", "UN")
        cur.setdefault("pack_qtd", 1)
        cur.setdefault("pack_unid", "UN")
        cur["grupo_codigo"] = grupo_codigo or cur.get("grupo_codigo") or ""
        itens.append(cur)
        cur = {}
        esperando = "fabricante"

    for line in _iter_lines(doc):
        # 1) grupo?
        mg = GRUPO_RE.match(line)
        if mg:
            # antes de trocar de grupo, fecha item pendente
            if cur:
                flush_item()
            grupo_codigo, grupo_titulo = mg.group(1).strip(), _clean(mg.group(2))
            grupos.append({"grupo_codigo": grupo_codigo, "grupo_titulo": grupo_titulo})
            continue

        # 2) pacote "C/ 12UN" pode vir depois da qtd
        mpk = PACK_RE.match(line)
        if mpk and cur:
            cur["pack_qtd"]  = int(mpk.group(1))
            cur["pack_unid"] = "UN"
            # não muda estado; pack é opcional
            continue

        # 3) FSM do item
        if esperando == "fabricante":
            # alguns mapas trazem um número (ex.: "22") em linhas sozinhas — ignorar
            if line.isdigit():
                continue
            if FAB_RE.match(line) and len(line) <= 30:
                cur = {"fabricante": line}
                esperando = "codigo"
                continue
            # às vezes fabricante não vem, mas já aparece código
            if COD_RE.match(line):
                cur = {"codigo": line}
                esperando = "ean"
                continue
            # se vier EAN direto (raro)
            if EAN_RE.match(line):
                cur = {"cod_barras": line}
                esperando = "descricao"
                continue
            # senão, talvez seja descrição (fallback)
            if len(line) > 3:
                cur = {"descricao": line}
                esperando = "qtd"
                continue

        elif esperando == "codigo":
            if COD_RE.match(line):
                cur["codigo"] = line
                esperando = "ean"
                continue
            # tolera fabricante repetido (alguns pdfs repetem)
            if FAB_RE.match(line):
                cur["fabricante"] = line
                continue
            # se veio descrição antes (sem EAN/código)
            if len(line) > 3 and not line.isdigit():
                cur["descricao"] = line
                esperando = "qtd"
                continue

        elif esperando == "ean":
            if EAN_RE.match(line):
                cur["cod_barras"] = line
                esperando = "descricao"
                continue
            # alguns itens não têm EAN; pode vir descrição direto
            if len(line) > 3 and not QTD_RE.match(line):
                cur["descricao"] = line
                esperando = "qtd"
                continue

        elif esperando == "descricao":
            # se chegou uma linha que parece quantidade, trata a anterior como descrição já fechada
            mq = QTD_RE.match(line)
            if mq:
                cur["qtd_unidades"] = int(mq.group(1))
                cur["unidade"] = mq.group(2).upper()
                flush_item()
                continue
            # senão, acumula/define descrição (alguns quebram em 2 linhas)
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
            # se não reconheceu quantidade, pode ter chegado um novo item; fecha o atual do jeito que der
            if GRUPO_RE.match(line) or FAB_RE.match(line) or COD_RE.match(line) or EAN_RE.match(line):
                flush_item()
                # reprocessa a linha atual como início do próximo
                # (empurra o estado de volta)
                if GRUPO_RE.match(line):
                    # já seria capturado no topo do loop numa próxima iteração
                    pass
                elif FAB_RE.match(line):
                    cur = {"fabricante": line}
                    esperando = "codigo"
                elif COD_RE.match(line):
                    cur = {"codigo": line}
                    esperando = "ean"
                elif EAN_RE.match(line):
                    cur = {"cod_barras": line}
                    esperando = "descricao"
                continue
            # se nada casa, anexa à descrição
            cur["descricao"] = (cur.get("descricao","") + " " + line).strip()

    # flush do último item
    if cur:
        flush_item()

    # header mínimo: tente capturar número da carga de alguma linha de grupo (se vier “1967 - ...”)
    if not header.get("numero_carga"):
        # heurística simples: se o primeiro grupo for o “cabeçalho” da carga, mas no seu caso você já traz “Mapa 1967” fora do PDF.
        pass

    return header, None, grupos, itens
