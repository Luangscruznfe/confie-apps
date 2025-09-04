# -*- coding: utf-8 -*-
"""
Parser robusto para 'Mapa de Separação' (PyMuPDF / fitz).
- Lê TODAS as páginas (sem cortar).
- Mantém a ordem visual (y, depois x).
- Faz flush do último item (não perde o item final).
- Retorna: header(dict), grupos(list[dict]), itens(list[dict])
"""

import re
from typing import Dict, List, Tuple, Any

try:
    import fitz  # PyMuPDF
except ImportError:
    raise RuntimeError("PyMuPDF (fitz) não encontrado. Instale com: pip install pymupdf")

# ----------------------------
# Utilidades de normalização
# ----------------------------

def _clean(s: str) -> str:
    if not s:
        return ""
    # remove separadores estranhos e espaços repetidos
    s = s.replace("\x0c", " ").replace("\u00ad", "")  # form feed e soft-hyphen
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()

def _to_int_safe(s: str) -> int:
    if s is None:
        return 0
    s = s.replace(".", "").replace(",", ".")
    m = re.search(r"-?\d+(\.\d+)?", s)
    if not m:
        return 0
    try:
        v = float(m.group(0))
        return int(round(v))
    except Exception:
        return 0

# ----------------------------
# Heurísticas de detecção
# ----------------------------

# 1) Cabeçalho: número da carga, data, cliente, etc. Ajuste conforme seu PDF.
_HEADER_HINTS = {
    "numero_carga": [r"(?:n[úu]mero|num\.?|nº)\s*da?\s*carga\s*[:\-]?\s*(\S+)", r"\bCARGA\s*[:\-]?\s*(\S+)"],
    "data": [r"\bdata\s*[:\-]?\s*([0-3]?\d\/[01]?\d\/\d{2,4})", r"\bEmiss[aã]o\s*[:\-]?\s*([0-3]?\d\/[01]?\d\/\d{2,4})"],
    "cliente": [r"\bcliente\s*[:\-]\s*(.+)$"],
}

# 2) Grupo: linha que indica início de seção (ex.: “GRUPO: HIGIENE” ou “Setor: …”)
_GRUPO_PATTERNS = [
    r"^\s*(?:GRUPO|SETOR|SEÇÃO|SECAO)\s*[:\-]\s*(.+?)\s*$",
]

# 3) Item: heurística flexível.
#    Aceita linhas iniciando com índice/código e que contenham uma descrição,
#    e normalmente terminam com QTD/UND ou pelo menos um número “quantidade”.
_ITEM_START_PATTERNS = [
    # 001 7891234567890 TOALHA ABSORV 2X55 BEST ... 12
    r"^\s*(\d{1,4})\s+(\d{6,})\s+(.+?)\s+(\d{1,6})\s*$",
    # 001 TOALHA ABSORV ... 12   (sem código de barras visível)
    r"^\s*(\d{1,4})\s+(.+?)\s+(\d{1,6})\s*$",
    # Código de barras primeiro
    r"^\s*(\d{6,})\s+(.+?)\s+(\d{1,6})\s*$",
]

# Caso o item quebre em várias linhas, juntamos até detectar a próxima âncora de item/grupo.
_NEXT_ANCHOR = re.compile(
    r"|".join(
        [
            _GRUPO_PATTERNS[0],
            # próxima linha com cara de item
            r"^\s*(\d{1,4})\s+(\d{6,})\s+.+\d\s*$",
            r"^\s*(\d{1,4})\s+.+\d\s*$",
            r"^\s*(\d{6,})\s+.+\d\s*$",
        ]
    ),
    re.IGNORECASE,
)

def _match_grupo(line: str) -> str:
    line = _clean(line)
    for pat in _GRUPO_PATTERNS:
        m = re.match(pat, line, flags=re.IGNORECASE)
        if m:
            return _clean(m.group(1))
    return ""

def _match_item(line: str) -> Dict[str, Any]:
    """
    Tenta casar linha de item com as heurísticas.
    Retorna dict com colunas básicas, ou {} se não casar.
    """
    s = _clean(line)

    # Tente padrões na ordem
    for pat in _ITEM_START_PATTERNS:
        m = re.match(pat, s, flags=re.IGNORECASE)
        if not m:
            continue

        groups = [g for g in m.groups()]

        # Normalização por quantidade de grupos identificados
        # Padrões acima cobrem 3 ou 4 grupos.
        if len(groups) == 4:
            idx, cod, desc, qtd = groups
            return {
                "indice": _to_int_safe(idx),
                "codigo": _clean(cod),
                "descricao": _clean(desc),
                "quantidade": _to_int_safe(qtd),
            }
        if len(groups) == 3:
            # Pode ser: (indice, desc, qtd)  OU  (cod, desc, qtd)
            g1, g2, g3 = groups
            if re.fullmatch(r"\d{1,4}", g1):  # índice pequeno
                return {
                    "indice": _to_int_safe(g1),
                    "codigo": "",
                    "descricao": _clean(g2),
                    "quantidade": _to_int_safe(g3),
                }
            else:
                # Supomos que g1 seja código de barras
                return {
                    "indice": 0,
                    "codigo": _clean(g1),
                    "descricao": _clean(g2),
                    "quantidade": _to_int_safe(g3),
                }

    return {}

# ----------------------------
# Extração por blocos (ordem visual)
# ----------------------------

def _iter_lines_in_reading_order(doc: "fitz.Document"):
    """
    Varre todas as páginas, pega blocos (page.get_text('blocks')),
    ordena por (y, x) e rende uma sequência de linhas limpas.
    """
    for page_index in range(doc.page_count):
        page = doc.load_page(page_index)
        blocks = page.get_text("blocks") or []

        # block: (x0, y0, x1, y1, text, block_no, block_type, ...)
        # Ordena por topo (y0) e depois por x0 para manter ordem de leitura
        blocks.sort(key=lambda b: (round(b[1], 2), round(b[0], 2)))

        for b in blocks:
            text = b[4] if len(b) > 4 else ""
            # Split por linhas; evita perder conteúdo por \x0c
            for raw_line in text.splitlines():
                line = _clean(raw_line)
                if line:
                    yield line

# ----------------------------
# Parser principal
# ----------------------------

def parse_mapa(pdf_path: str) -> Tuple[Dict[str, str], List[Dict[str, str]], List[Dict[str, Any]]]:
    """
    Retorna:
      header: { 'numero_carga': str, 'data': 'dd/mm/aaaa', 'cliente': '...' }
      grupos: [ { 'nome': 'HIGIENE' }, ... ]
      itens:  [ { 'grupo': 'HIGIENE', 'indice': 1, 'codigo': '789...', 'descricao': '...', 'quantidade': 12 }, ... ]
    """
    doc = fitz.open(pdf_path)

    header: Dict[str, str] = {}
    grupos: List[Dict[str, str]] = []
    itens: List[Dict[str, Any]] = []

    # Estado corrente
    grupo_atual = ""
    buffer_item_lines: List[str] = []  # para itens quebrados em várias linhas

    # 1) Varre linhas em ordem de leitura
    lines = list(_iter_lines_in_reading_order(doc))

    # 2) Primeiro, tenta capturar cabeçalho nas 60 primeiras linhas (ajuste se precisar)
    header_region = "\n".join(lines[:60])
    for key, patterns in _HEADER_HINTS.items():
        for pat in patterns:
            m = re.search(pat, header_region, flags=re.IGNORECASE | re.MULTILINE)
            if m:
                header[key] = _clean(m.group(1))
                break
        header.setdefault(key, "")

    # 3) Passa item a item, detectando grupos e itens
    def _flush_buffer_item():
        """Tenta consolidar o que estiver no buffer como um item."""
        nonlocal buffer_item_lines, grupo_atual, itens
        if not buffer_item_lines:
            return

        joined = _clean(" ".join(buffer_item_lines))
        data = _match_item(joined)
        if data:
            data["grupo"] = grupo_atual
            itens.append(data)
        buffer_item_lines = []

    for line in lines:
        # Detecta grupo
        grupo = _match_grupo(line)
        if grupo:
            # Antes de trocar de grupo, garante flush de item pendente
            _flush_buffer_item()

            grupo_atual = grupo
            grupos.append({"nome": grupo_atual})
            continue

        # Tentativa de match direto de item
        item = _match_item(line)
        if item:
            # se já havia lixo no buffer (linhas do item anterior), fecha antes
            _flush_buffer_item()
            item["grupo"] = grupo_atual
            itens.append(item)
            continue

        # Se não é grupo nem item direto, pode ser continuação do item atual
        if buffer_item_lines:
            # Se a próxima linha parece um novo anchor (novo item ou novo grupo), fecha o buffer
            if _NEXT_ANCHOR.match(line):
                _flush_buffer_item()
                # reprocessa essa linha como possível novo início
                # (chamando lógicas acima)
                grupo = _match_grupo(line)
                if grupo:
                    grupo_atual = grupo
                    grupos.append({"nome": grupo_atual})
                    continue
                item = _match_item(line)
                if item:
                    item["grupo"] = grupo_atual
                    itens.append(item)
                    continue
                # se não casou nada, começa um novo buffer com essa linha
                buffer_item_lines = [line]
            else:
                buffer_item_lines.append(line)
        else:
            # buffer vazio: talvez seja o começo de um item quebrado
            # regra simples: se contém número + texto, guardamos
            if re.search(r"\d", line) and re.search(r"[A-Za-zÀ-ÖØ-öø-ÿ]", line):
                buffer_item_lines.append(line)
            # senão, ignoramos (linhas soltas de layout)

    # 4) MUITO IMPORTANTE: flush final para não perder o último item
    _flush_buffer_item()

    # 5) Pós-processamento simples: remove itens vazios e normaliza
    itens = [
        {
            "grupo": i.get("grupo", ""),
            "indice": int(i.get("indice", 0) or 0),
            "codigo": _clean(i.get("codigo", "")),
            "descricao": _clean(i.get("descricao", "")),
            "quantidade": int(i.get("quantidade", 0) or 0),
        }
        for i in itens
        if _clean(i.get("descricao", "")) or _clean(i.get("codigo", ""))
    ]

        return header, None, grupos, itens

# ----------------------------
# Função de debug (opcional)
# ----------------------------

def debug_mapa(pdf_path: str) -> None:
    """
    Use esta função para inspecionar rapidamente o que o parser está vendo.
    """
    h, g, it = parse_mapa(pdf_path)
    print("[HEADER]")
    for k, v in h.items():
        print(f"  {k}: {v}")

    print("\n[GRUPOS]")
    for idx, gg in enumerate(g, 1):
        print(f"  {idx:02d}. {gg['nome']}")

    print(f"\n[ITENS] total={len(it)}")
    for i in it[-5:]:  # mostra os últimos 5 pra checar o 'último item'
        print(f"  ({i.get('grupo')}) #{i.get('indice')} {i.get('codigo')}  {i.get('descricao')}  QTD={i.get('quantidade')}")


def debug_extrator(pdf_path: str):
    """
    Retorna uma lista de linhas lidas com um parse básico por linha,
    no formato: [{ "n": int, "line": str, "parsed": dict|{} }, ...]
    """
    doc = fitz.open(pdf_path)
    rows = []
    n = 0
    for line in _iter_lines_in_reading_order(doc):
        n += 1
        parsed = {}
        g = _match_grupo(line)
        if g:
            parsed = {"grupo": g}
        else:
            it = _match_item(line)
            if it:
                parsed = it
        rows.append({"n": n, "line": line, "parsed": parsed})
    doc.close()
    return rows

