# parser_mapa.py (versão robusta e corrigida para packs)
import re, fitz

HEAD_IGNORES = (
    "SEPARAÇÃO DE CARGA",
    "FABRIC.CÓDIGO", "FABRIC.CODIGO",
    "CÓD. BARRAS", "COD. BARRAS",
    "PAG.:",
    "DATA EMISSÃO", "MOTORISTA",
    "PESO TOTAL", "ENTREGAS",
    "PEDIDOS:", "NÚMERO DA CARGA", "NUMERO DA CARGA"
)

# Unidades de picking aceitas (quantidade separada)
PICK_UNIDADES = {"UN","CX","FD","CJ","DP","PC","PT","DZ","SC","KT","JG","BF","PA"}
# Unidades de peso/volume que NÃO são picking (vão ficar na descrição)
WEIGHT_UNIDADES = {"G","KG","ML","L"}

# Estes regex auxiliares não serão mais usados diretamente por try_parse_line,
# mas podem ser úteis para outras validações ou debug.
PACK_ONLY_RE = re.compile(r"^C/\s*(\d+)\s*([A-Z]{1,4})$", re.IGNORECASE)
QTY_ONLY_RE  = re.compile(r"^(\d+)\s*([A-Z]{1,4})$", re.IGNORECASE)

def _is_only_pack(s: str):
    m = PACK_ONLY_RE.match(" ".join((s or "").strip().split()))
    if not m:
        return None
    return int(m.group(1)), (m.group(2) or "").upper()

def _is_only_qty(s: str):
    m = QTY_ONLY_RE.match(" ".join((s or "").strip().split()))
    if not m:
        return None
    un = (m.group(2) or "").upper()
    if un not in PICK_UNIDADES:   # ignora peso/volume como “quantidade”
        return None
    return int(m.group(1)), un

# =========================
# 1) Cabeçalho (tolerante)
# =========================
HEADER_PATTERNS = {
    "numero_carga": re.compile(
        r"(?:N[uú]mero\s+da\s+Carga|Numero\s+da\s+Carga|N[º°]\s*da?\s*Carg[ae])\s*:\s*([A-Za-z0-9\-\/\.]+)",
        re.IGNORECASE
    ),
    "motorista": re.compile(r"Motorista\s*:\s*(.+)", re.IGNORECASE),
    "descricao_romaneio": re.compile(r"Desc\.?\s*Romaneio\s*:\s*(.+)", re.IGNORECASE),
    "peso_total": re.compile(r"Peso\s+Total\s*:\s*([\d\.,]+)", re.IGNORECASE),
    "entregas": re.compile(r"Entregas\s*:\s*(\d+)", re.IGNORECASE),
    "data_emissao": re.compile(r"Data(?:\s+de)?\s*Emiss[aã]o\s*:\s*([\d/]{8,10})", re.IGNORECASE),
}

RE_PEDIDOS_INICIO = re.compile(r"^Pedidos:\s*(.*)", re.IGNORECASE)
RE_GRUPO = re.compile(r"^([A-Z]{3}\d+)\s*-\s*(.+)$", re.IGNORECASE)

# =========================
# 2) Padrões de itens
# =========================
# Padrão “completo” (com EAN), aceita "C/ 12UN" opcional
RE_ITEM = re.compile(
    r"""^(?:C/\s*(?P<pack_qtd>\d+)\s*(?P<pack_unid>[A-Z]+))?\s*     # prefixo opcional "C/ 12UN"
        (?P<fabricante>[A-Z0-9À-Ú\-&\. ]+?)\s+                      # fabricante (com acento)
        (?P<codigo>\d{3,})\s+                                      # código interno (3+ dígitos)
        (?P<cod_barras>\d{8,14})\s+                                # EAN/GTIN (8–14 dígitos)
        (?P<descricao>.+?)\s+                                      # descrição
        (?P<qtd_unidades>\d+)\s*(?P<unidade>[A-Z]{1,4})\s*$        # quantidade e unidade
    """,
    re.VERBOSE | re.IGNORECASE
)

# Padrão “flex” (quando falta EAN ou fabricante, ou a ordem muda um pouco)
# Exemplos que esse padrão cobre:
#   "C/ 12UN 24916  BARRA SUCRILHOS CHOCOLATE  1 DP"
#   "KELLANOVA 24916 BARRA... 1 DP" (sem EAN)
#   "24916 7896004004495 BARRA... 1 DP" (sem fabricante)
RE_ITEM_FLEX = re.compile(
    r"""^(?:C/\s*(?P<pack_qtd>\d+)\s*(?P<pack_unid>[A-Z]+))?\s*
        (?:(?P<fabricante>[A-Z0-9À-Ú\-&\. ]+?)\s+)?                 # fabricante opcional
        (?P<codigo>\d{3,})\s+                                      # código
        (?:(?P<cod_barras>\d{8,14})\s+)?                           # EAN opcional
        (?P<descricao>.+?)\s+                                      # descrição
        (?P<qtd_unidades>\d+)\s*(?P<unidade>[A-Z]{1,4})\s*$        # qtd/unidade
    """,
    re.VERBOSE | re.IGNORECASE
)

# =========================
# 3) Extração robusta
# =========================
def extract_text_from_pdf(path_pdf: str) -> str:
    """
    1) Tenta 'text' normal.
    2) Se vier pouco texto, reconstrói por 'words' (ordena por y/x e agrupa por linha).
    3) Se ainda curto, tenta 'blocks'.
    """
    doc = fitz.open(path_pdf)
    pages_text = []

    def rebuild_from_words(page):
        words = page.get_text("words")  # [x0,y0,x1,y1,"texto",block,line,word]
        if not words:
            return ""
        words.sort(key=lambda w: (round(w[1], 1), w[0]))  # y, depois x
        lines, current_y, buf = [], None, []
        for w in words:
            y = round(w[1], 1)
            if current_y is None:
                current_y = y
            if abs(y - current_y) > 1.5:  # nova linha
                if buf:
                    lines.append(" ".join(buf))
                buf = []
                current_y = y
            buf.append(w[4])
        if buf:
            lines.append(" ".join(buf))
        return "\n".join(lines)

    for page in doc:
        t = (page.get_text("text") or "").replace("\xa0", " ").strip()
        if len(t) < 50:
            t = rebuild_from_words(page)
        if len(t) < 50:
            try:
                blocks = page.get_text("blocks") or []
                t = "\n".join((b[4] or "").strip() for b in blocks if len(b) >= 5)
            except:
                pass
        pages_text.append(t)
    doc.close()
    return "\n".join(pages_text)

# =========================
# 4) Parse do cabeçalho/pedidos
# =========================
def parse_header_and_pedidos(all_text: str):
    header = {}
    for k, rgx in HEADER_PATTERNS.items():
        m = rgx.search(all_text)
        if m:
            header[k] = (m.group(1) or "").strip()

    pedidos, cap, cap_on = [], [], False
    for line in all_text.splitlines():
        ls = line.strip()
        if not cap_on:
            m = RE_PEDIDOS_INICIO.match(ls)
            if m:
                cap_on = True
                cap.append(m.group(1))
        else:
            if ls.lower().startswith("fabric.") or RE_GRUPO.match(ls):
                break
            cap.append(ls)

    if cap:
        blob = " ".join(cap).replace("Pedidos:", " ")
        pedidos = re.findall(r"\d{3,}", blob)
    return header, pedidos

# =========================
# 5) Parse de grupos/itens
# =========================

def try_parse_line(line: str):
    """
    Tenta parsear UMA linha de item usando os padrões RE_ITEM e RE_ITEM_FLEX.
    Prioriza RE_ITEM (mais completo) e depois RE_ITEM_FLEX.
    """
    s = " ".join((line or "").strip().split())
    if not s:
        return None

    # Tenta casar com o padrão mais completo primeiro
    match = RE_ITEM.match(s)
    if not match:
        # Se não casar, tenta com o padrão flexível
        match = RE_ITEM_FLEX.match(s)

    if match:
        # Extrai os grupos nomeados do match
        data = match.groupdict()

        # Converte quantidades para int, se existirem
        # Usa .get() para lidar com grupos que podem não existir em RE_ITEM_FLEX
        data['qtd_unidades'] = int(data['qtd_unidades']) if data.get('qtd_unidades') else 0
        data['pack_qtd'] = int(data['pack_qtd']) if data.get('pack_qtd') else 0

        # Garante que unidades e pack_unid sejam maiúsculas
        data['unidade'] = (data.get('unidade') or '').upper()
        data['pack_unid'] = (data.get('pack_unid') or '').upper()

        # Filtra unidades que não são de picking para não serem consideradas como 'unidade' principal
        # Se a unidade não é de picking, a qtd_unidades também não faz sentido aqui
        if data['unidade'] not in PICK_UNIDADES:
            data['unidade'] = ''
            data['qtd_unidades'] = 0

        # Retorna apenas os campos relevantes e limpos
        return {
            "fabricante": (data.get('fabricante') or '').strip().upper(),
            "codigo": (data.get('codigo') or '').strip(),
            "cod_barras": (data.get('cod_barras') or '').strip(),
            "descricao": (data.get('descricao') or '').strip(),
            "qtd_unidades": data['qtd_unidades'],
            "unidade": data['unidade'],
            "pack_qtd": data['pack_qtd'],
            "pack_unid": data['pack_unid'],
        }
    return None # Não houve match

def parse_groups_and_items(all_text: str):
    grupos, itens, current_group = [], [], None
    lines = [" ".join(l.strip().split()) for l in all_text.splitlines() if l.strip()]
    i = 0
    while i < len(lines):
        line = lines[i]

        # >>> IGNORA cabeçalhos/rodapés do PDF
        upper = line.upper()
        if any(token in upper for token in HEAD_IGNORES):
            i += 1
            continue

        # Grupo
        mg = RE_GRUPO.match(line)
        if mg:
            current_group = {"codigo": mg.group(1).upper(), "titulo": mg.group(2).strip()}
            grupos.append(current_group)
            # ao trocar de grupo, limpa pendentes
            pending_qty = None
            pending_pack = None
            i += 1
            continue

        if not current_group:
            i += 1
            continue

        # Se a linha é só QTD/UN ou só PACK, guarda no buffer e segue
        only_q = _is_only_qty(line)
        if only_q:
            pending_qty = only_q
            i += 1
            continue
        only_p = _is_only_pack(line)
        if only_p:
            pending_pack = only_p
            i += 1
            continue

        # 1) tenta parsear a linha atual
        parsed = try_parse_line(line)

        # 2) quebra de descrição? tenta juntar com a próxima
        if not parsed and i + 1 < len(lines):
            join_next = f"{line} {lines[i+1]}"
            parsed = try_parse_line(join_next)
            if parsed:
                i += 1  # consumiu a próxima linha

        if parsed:
            # 3) aplica buffers pendentes (caso pack/qtd tenham aparecido ANTES do item)
            if not parsed.get("qtd_unidades") and pending_qty:
                parsed["qtd_unidades"], parsed["unidade"] = pending_qty
                pending_qty = None
            if not parsed.get("pack_qtd") and pending_pack:
                parsed["pack_qtd"], parsed["pack_unid"] = pending_pack
                pending_pack = None

            # 4) olha LINHAS SEGUINTES imediatas para completar qtd e pack
            # Esta lógica pode ser simplificada ou removida se try_parse_line for robusto o suficiente
            # para pegar tudo em uma única linha.
            if i + 1 < len(lines):
                nxt = lines[i+1]
                got = False
                qn = _is_only_qty(nxt)
                if qn and not parsed.get("qtd_unidades"):
                    parsed["qtd_unidades"], parsed["unidade"] = qn
                    i += 1
                    got = True
                if got and i + 1 < len(lines):
                    nx2 = lines[i+1]
                    pk2 = _is_only_pack(nx2)
                    if pk2 and not parsed.get("pack_qtd"):
                        parsed["pack_qtd"], parsed["pack_unid"] = pk2
                        i += 1
                if not got:
                    pk = _is_only_pack(nxt)
                    if pk and not parsed.get("pack_qtd"):
                        parsed["pack_qtd"], parsed["pack_unid"] = pk
                        i += 1

            parsed["grupo_codigo"] = current_group["codigo"]
            if parsed["descricao"] and (parsed["codigo"] or parsed["fabricante"] or parsed["cod_barras"]):
                itens.append(parsed)

        i += 1

    return grupos, itens

# =========================
# 6) Orquestração
# =========================
def parse_mapa(path_pdf: str):
    text = extract_text_from_pdf(path_pdf)
    header, pedidos = parse_header_and_pedidos(text)
    grupos, itens = parse_groups_and_items(text)

    # Fallback: tenta extrair número da carga do nome do arquivo
    if not header.get("numero_carga"):
        fname = (path_pdf or "").split("/")[-1]
        m = re.search(r"(\d{4,})", fname)
        if m:
            header["numero_carga"] = m.group(1)

    if not header.get("numero_carga"):
        raise ValueError("Não encontrei 'Número da Carga' no PDF (tentei variações).")
    if not itens:
        raise ValueError("Não encontrei itens no PDF.")

    return header, pedidos, grupos, itens

def debug_extrator(path_pdf: str):
    """Devolve as linhas cruas e como cada uma foi parseada (ou não)."""
    # Importação local para evitar circularidade se este arquivo for importado por outro
    # que também importa debug_extrator.
    from .parser_mapa import extract_text_from_pdf # Ajustado para importação relativa
    raw = extract_text_from_pdf(path_pdf)
    lines = [l for l in raw.splitlines()]
    out = []
    for i, ln in enumerate(lines, 1):
        parsed = try_parse_line(ln)
        out.append({"n": i, "line": ln, "parsed": parsed})
    return out
