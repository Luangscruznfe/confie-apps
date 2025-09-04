# -*- coding: utf-8 -*-
import re
from typing import Dict, List, Tuple, Any

try:
    import fitz  # PyMuPDF
except ImportError:
    raise RuntimeError("PyMuPDF (fitz) não encontrado. Instale com: pip install pymupdf")

# ===== Constantes de Layout (Ajuste se o PDF mudar) =====
# Coordenada X onde a coluna do Fabricante começa
X_FABRICANTE = 430
# Coordenada X onde a coluna da Quantidade começa
X_QUANTIDADE = 500

# Regex para identificar uma linha de grupo (ex: "GBA1-BALAS/GOMAS")
GRUPO_RE = re.compile(r"^[A-Z0-9]{3,}\s*-\s*[A-Z0-9/\s]+$")


# ---------- utils ----------
def _clean(s: str) -> str:
    """Limpa a string de caracteres indesejados e espaços múltiplos."""
    if not s:
        return ""
    s = s.replace("\x0c", " ").replace("\u00ad", "")
    return re.sub(r"\s+", " ", s).strip()

# ===== PARSER PRINCIPAL (LÓGICA REESCRITA USANDO COORDENADAS) =====

def parse_mapa(pdf_path: str) -> Tuple[Dict[str, str], Any, List[Dict[str, str]], List[Dict[str, Any]]]:
    """
    Nova versão do parser, adaptada para o layout colunar,
    utilizando a mesma técnica do extrator de conferência de pedidos.
    """
    doc = fitz.open(pdf_path)
    header: Dict[str, str] = {}
    grupos: List[Dict[str, str]] = []
    itens: List[Dict[str, Any]] = []
    grupo_codigo_atual = ""

    for page_num, page in enumerate(doc):
        # 1. Extração do Cabeçalho (apenas na primeira página)
        if page_num == 0:
            text = page.get_text("text")
            m = re.search(r"N[uú]mero da Carga:\s*(\d+)", text, re.I)
            if m: header["numero_carga"] = m.group(1).strip()
            
            m = re.search(r"Data Emiss[aã]o:\s*([\d/]+)", text, re.I)
            if m: header["data"] = m.group(1).strip()
            
            m = re.search(r"Motorista:\s*(.+)", text, re.I)
            if m: header["motorista"] = _clean(m.group(1))

            m = re.search(r"Desc\.?\s*Romaneio:\s*(.+)", text, re.I)
            if m: header["romaneio"] = _clean(m.group(1))

        # 2. Extração dos Itens da Tabela (todas as páginas)
        words = page.get_text("words")
        if not words:
            continue
            
        # Agrupa palavras em linhas baseadas na coordenada Y
        lines = {}
        for w in words:
            x0, y0, x1, y1, text = w[:5]
            y_key = round(y0)
            if y_key not in lines:
                lines[y_key] = []
            lines[y_key].append(w)

        # Processa cada linha
        for y_key in sorted(lines.keys()):
            line_words = sorted(lines[y_key], key=lambda w: w[0]) # Ordena por X
            
            desc_parts, fab_parts, qtd_parts = [], [], []
            
            for x0, y0, x1, y1, text, *_ in line_words:
                if "Cód. Barras" in text or "Código Descrição" in text:
                    break # Ignora a linha de cabeçalho da tabela
                
                if x0 < X_FABRICANTE:
                    desc_parts.append(text)
                elif x0 < X_QUANTIDADE:
                    fab_parts.append(text)
                else:
                    qtd_parts.append(text)
            else: # Executa se o loop for concluído (sem break)
                full_desc = _clean(" ".join(desc_parts))
                fabricante = _clean(" ".join(fab_parts))
                quantidade = _clean(" ".join(qtd_parts))

                if not full_desc:
                    continue

                # É uma linha de grupo?
                if GRUPO_RE.match(full_desc) and not fabricante and not quantidade:
                    partes_grupo = [p.strip() for p in full_desc.split('-', 1)]
                    if len(partes_grupo) == 2:
                        grupo_codigo_atual = partes_grupo[0]
                        if not any(g['grupo_codigo'] == grupo_codigo_atual for g in grupos):
                             grupos.append({"grupo_codigo": grupo_codigo_atual, "grupo_titulo": partes_grupo[1]})
                    continue

                # É uma linha de item
                item = {
                    "grupo_codigo": grupo_codigo_atual,
                    "fabricante": fabricante,
                    "quantidade_str": quantidade # Salva a string original da quantidade
                }
                
                # Trata casos onde o grupo e o item estão na mesma linha
                match_grupo_inline = re.match(r"([A-Z0-9]{3,}\s*-\s*[A-Z0-9/\s]+)\s*(.*)", full_desc)
                if match_grupo_inline:
                    grupo_inline_str = match_grupo_inline.group(1)
                    full_desc = match_grupo_inline.group(2) # O resto é a descrição
                    
                    partes_grupo = [p.strip() for p in grupo_inline_str.split('-', 1)]
                    if len(partes_grupo) == 2:
                        grupo_codigo_atual = partes_grupo[0]
                        item["grupo_codigo"] = grupo_codigo_atual
                        if not any(g['grupo_codigo'] == grupo_codigo_atual for g in grupos):
                            grupos.append({"grupo_codigo": grupo_codigo_atual, "grupo_titulo": partes_grupo[1]})
                
                # Separa EAN, Código e Descrição
                desc_words = full_desc.split()
                if desc_words:
                    if re.match(r"^\d{12,14}$", desc_words[0]):
                        item["cod_barras"] = desc_words.pop(0)
                    
                    if desc_words and re.match(r"^\d{3,}$", desc_words[0]):
                        item["codigo"] = desc_words.pop(0)

                item["descricao"] = " ".join(desc_words)

                # Extrai unidades do pacote e quantidade principal
                match_unidade = re.match(r'(\d+)\s*([A-Z]+)', quantidade)
                if match_unidade:
                    item["qtd_unidades"] = int(match_unidade.group(1))
                    item["unidade"] = match_unidade.group(2).upper()
                
                match_pack = re.search(r'C/\s*(\d+)', quantidade, re.I)
                if match_pack:
                    item["pack_qtd"] = int(match_pack.group(1))

                # Adiciona à lista final se tivermos conseguido extrair algo
                if item.get("descricao") or item.get("codigo"):
                    itens.append(item)

    doc.close()
    return header, None, grupos, itens


# ---------- Função de depuração (mantida para testes futuros) ----------
def debug_extrator(pdf_path: str):
    """Retorna linhas para inspeção rápida."""
    doc = fitz.open(pdf_path)
    rows = []
    # Adapte se precisar de uma depuração mais detalhada no futuro
    for page in doc:
        rows.extend(page.get_text("text").splitlines())
    doc.close()
    return [{"n": i+1, "line": line, "parsed": {}} for i, line in enumerate(rows)]