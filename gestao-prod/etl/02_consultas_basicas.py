r"""
PARTE 2 (revisado, AUTOCONTIDO) — Consultas básicas por OP (somente leitura)

- Usa o driver 100% Python `firebirdsql` (sem fbclient).
- Lê variáveis do arquivo etl\.env (FIREBIRD_HOST, FIREBIRD_DB_PATH, etc).
- Funciona mesmo com variações de nomes de colunas/tabelas:
  * ORDEM_PRODUCAO_ITENS com colunas OPD_*
  * Roteiro (tenta detectar tabela e colunas automaticamente)

Como rodar (no PowerShell, dentro da pasta do projeto):
  (.venv) PS C:\...\gestao-prod\etl> python .\02_consultas_basicas.py

Pré-requisitos:
  pip install firebirdsql python-dotenv

ATENÇÃO: leitura SOMENTE. Nada é escrito no MSYSDADOS.FDB.
"""

import os
from typing import List, Tuple, Any, Dict, Optional
from datetime import datetime
import re

import firebirdsql
from dotenv import load_dotenv

# ======================================================================================
# Configuração de ambiente (.env)
# ======================================================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(ENV_PATH)

FB_HOST = os.getenv("FIREBIRD_HOST", "localhost")   # ex.: SRVFERROSUL
FB_PORT = int(os.getenv("FIREBIRD_PORT", "3050"))
FB_DB   = os.getenv("FIREBIRD_DB_PATH")             # ex.: C:\Microsys\...\MSYSDADOS.FDB (caminho visto pelo servidor) ou alias
FB_USER = os.getenv("FIREBIRD_USER", "SYSDBA")
FB_PASS = os.getenv("FIREBIRD_PASSWORD", "masterkey")
FB_CHAR = os.getenv("FIREBIRD_CHARSET", "WIN1252")  # comum em instalações Microsys

# ======================================================================================
# Conexão e helpers genéricos
# ======================================================================================

def connect_fb():
    if not FB_DB:
        raise SystemExit("Erro: defina FIREBIRD_DB_PATH em etl\\.env")
    return firebirdsql.connect(
        host=FB_HOST, port=FB_PORT, database=FB_DB,
        user=FB_USER, password=FB_PASS, charset=FB_CHAR
    )

def list_user_tables(cur) -> List[str]:
    cur.execute("""
        SELECT TRIM(r.rdb$relation_name)
        FROM rdb$relations r
        WHERE r.rdb$system_flag = 0
          AND r.rdb$view_blr IS NULL
        ORDER BY 1
    """)
    return [row[0] for row in cur.fetchall()]

def table_has_column(cur, table: str, column: str) -> bool:
    cur.execute("""
        SELECT COUNT(*)
        FROM rdb$relation_fields rf
        WHERE rf.rdb$relation_name = ?
          AND rf.rdb$field_name = ?
    """, (table, column))
    return cur.fetchone()[0] > 0

def list_columns(cur, table: str) -> List[str]:
    cur.execute("""
        SELECT TRIM(rf.rdb$field_name)
        FROM rdb$relation_fields rf
        WHERE rf.rdb$relation_name = ?
        ORDER BY rf.rdb$field_position
    """, (table,))
    return [row[0] for row in cur.fetchall()]

def safe_fetchall(cur, sql: str, params: Tuple = ()) -> Tuple[List[str], List[Tuple]]:
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return cols, rows

def print_rows(title: str, cols: List[str], rows: List[Tuple], limit: int = 10):
    print(f"\n=== {title} ===")
    print("Colunas:", cols)
    if not rows:
        print("(sem registros)")
        return
    for i, r in enumerate(rows[:limit], 1):
        vals = []
        for c, v in zip(cols, r):
            if isinstance(v, datetime):
                v = v.strftime("%Y-%m-%d %H:%M:%S")
            vals.append(f"{c}={v}")
        print(f"[{i}] " + "; ".join(vals))

# ======================================================================================
# Mapas (status e setores) — portados de status.ts e setores.map.ts
# ======================================================================================

def map_legacy_status(code: Optional[str]) -> str:
    """
    Converte código legado para nome claro.
    """
    c = (code or "").strip().upper()
    if c == "AA": return "ABERTA"
    if c == "IN": return "INICIADA"
    if c in ("EP", "SS"): return "ENTRADA PARCIAL"
    if c == "FF": return "FINALIZADA"
    if c == "CC": return "CANCELADA"
    return "OUTRO"

SETOR_LEGACY_MAP: Dict[int, str] = {
    1: "Perfiladeira",
    3: "Serralheria",
    4: "Pintura",
    6: "Eixo",
    # adicione aqui quando souber outros códigos -> nomes
}

# ======================================================================================
# Descoberta automática do ROTEIRO (port simplificado do integracao.repo.ts)
# ======================================================================================

def _pick_col(cols: List[str], preferred: List[str], patterns: List[re.Pattern]) -> Optional[str]:
    upp = [c.upper() for c in cols]
    for cand in preferred:
        u = cand.upper()
        if u in upp:
            return u
    for rx in patterns:
        for c in upp:
            if rx.search(c):
                return c
    return None

def resolve_roteiro_columns(cur) -> Optional[Dict[str, str]]:
    """
    Tenta descobrir tabela e nomes de colunas do roteiro:
      - Coluna que liga à OP (ID ou SERIE)
      - Coluna do código do setor
      - Coluna de sequência/ordem
    Retorna dict {TABLE, OP_NUM, SETOR_COD, SEQ} ou None.
    """
    table_candidates = [
        "PCP_APTO_ROTEIRO",
        "PCP_ORP_ROTEIRO",
        "PCP_ROTEIRO",
        "ROTEIRO",
    ]
    op_num_pref = ["OPR_ORP_NUMERO","APR_ORP_NUMERO","OPR_ORP_SERIE","APR_ORP_SERIE","ORP_NUMERO","ORP_SERIE","OPR_ORP_ID","APR_ORP_ID"]
    op_num_regex = [re.compile(r"(^|_)ORP_?(NUM|NUMERO|SERIE|ID)$", re.I)]

    setor_pref = ["OPR_ATV_ID","APR_ATV_ID","OPR_SET_CODIGO","APR_SET_CODIGO","ATV_ID","ATV_CODIGO","SET_CODIGO","SETOR_CODIGO"]
    setor_regex = [re.compile(r"ATV.*(ID|COD)", re.I), re.compile(r"SET.*COD", re.I), re.compile(r"SETOR", re.I)]

    seq_pref = ["OPR_ATV_SEQUENCIA","APR_ATV_SEQUENCIA","ATV_SEQUENCIA","SEQUENCIA","ORDEM","OPR_SEQ"]
    seq_regex = [re.compile(r"SEQ", re.I), re.compile(r"ORDEM", re.I)]

    tabs = list_user_tables(cur)
    for t in table_candidates:
        if t not in tabs:
            continue
        cols = list_columns(cur, t)
        op_num = _pick_col(cols, op_num_pref, op_num_regex)
        setor  = _pick_col(cols, setor_pref,  setor_regex)
        seq    = _pick_col(cols, seq_pref,    seq_regex)
        if op_num and setor and seq:
            return {"TABLE": t, "OP_NUM": op_num, "SETOR_COD": setor, "SEQ": seq}
    return None

# ======================================================================================
# Consultas por OP
# ======================================================================================

def listar_ops_recentes(cur, limite: int = 15) -> List[Dict[str, Any]]:
    """
    Lista OPs recentes com campos úteis.
    Ordena por ORP_DT_VALIDADE se existir; senão por ORP_DATA.
    """
    order_col = "ORP_DT_VALIDADE" if table_has_column(cur, "ORDEM_PRODUCAO", "ORP_DT_VALIDADE") else "ORP_DATA"
    cols_try = [
        "ORP_ID","ORP_SERIE","ORP_DESCRICAO","ORP_PDV_NUMERO","ORP_DOC_PRIORIDADE",
        "ORP_STS_CODIGO","ORP_STS_ID","ORP_DATA","ORP_DT_VALIDADE","ORP_DT_PREV_INICIO"
    ]
    existing = [c for c in cols_try if table_has_column(cur, "ORDEM_PRODUCAO", c)]
    select_cols = ", ".join(existing) if existing else "ORP_ID, ORP_SERIE, ORP_DESCRICAO"
    sql = f"SELECT FIRST {limite} {select_cols} FROM ORDEM_PRODUCAO ORDER BY {order_col} DESC"
    cols, rows = safe_fetchall(cur, sql)
    print_rows(f"OPs recentes (ordenado por {order_col})", cols, rows)
    return [dict(zip(cols, r)) for r in rows]

def cabecalho_op(cur, op_id: Any):
    """
    Mostra cabeçalho da OP e retorna (cols, rows).
    """
    cols, rows = safe_fetchall(cur, """
        SELECT
          ORP_ID, ORP_SERIE, ORP_DESCRICAO, ORP_PDV_NUMERO,
          ORP_DATA, ORP_DT_VALIDADE, ORP_DT_PREV_INICIO,
          ORP_STS_CODIGO, ORP_STS_ID, ORP_OBSERVACAO,
          ORP_QTDE_PRODUCAO, ORP_QTDE_PRODUZIDAS, ORP_QTDE_SALDO
        FROM ORDEM_PRODUCAO
        WHERE ORP_ID = ?
    """, (op_id,))
    print_rows("ORDEM_PRODUCAO (cabeçalho)", cols, rows)
    return cols, rows

def itens_da_op(cur, op_id: Any, orp_serie: Any):
    """
    Busca itens por OP tentando ambos vínculos:
      1) OPD_ORP_ID     = ORP_ID
      2) OPD_ORP_SERIE  = ORP_SERIE
    """
    table = "ORDEM_PRODUCAO_ITENS"
    tabs = list_user_tables(cur)
    if table not in tabs:
        # tenta detectar variações do nome
        alts = [t for t in tabs if "ORDEM" in t.upper() and "PRODU" in t.upper() and "ITEN" in t.upper()]
        if not alts:
            print("\n=== ORDEM_PRODUCAO_ITENS ===\n(Tabela não encontrada.)")
            return [], []
        table = alts[0]

    base_cols = ["OPD_ID","OPD_ORP_ID","OPD_ORP_SERIE","OPD_LOTE","OPD_PRO_CODIGO",
                 "OPD_QUANTIDADE","OPD_QTD_PRODUZIDAS","OPD_QTDE_SALDO","OPD_COR_CODIGO"]
    have = [c for c in base_cols if table_has_column(cur, table, c)]

    # via ORP_ID
    if "OPD_ORP_ID" in have:
        cols, rows = safe_fetchall(cur,
            f"SELECT {', '.join(have)} FROM {table} WHERE OPD_ORP_ID = ?",
            (op_id,))
        if rows:
            print_rows(f"{table} (via OPD_ORP_ID)", cols, rows)
            return cols, rows

    # via ORP_SERIE
    if "OPD_ORP_SERIE" in have:
        cols, rows = safe_fetchall(cur,
            f"SELECT {', '.join(have)} FROM {table} WHERE OPD_ORP_SERIE = ?",
            (orp_serie,))
        print_rows(f"{table} (via OPD_ORP_SERIE)", cols, rows)
        return cols, rows

    print(f"\n=== {table} ===\n(Não encontrei colunas de vínculo esperadas.)")
    return [], []

def cor_e_percentual(cur, orp_serie: Any, hdr_qtds: Dict[str, Optional[float]]):
    """
    Replica a lógica já testada:
      - % concluído calculado preferencialmente pela SOMA dos ITENS (saldo/total);
      - se não houver itens, cai para os campos do cabeçalho;
      - cor por OP: COALESCE(cor_do_item, cor_do_produto) agregado (LIST DISTINCT).
    """
    # Totais por itens
    it_cols, it_rows = safe_fetchall(cur, """
        SELECT
          i.OPD_ORP_SERIE AS ORP_SERIE,
          SUM(COALESCE(i.OPD_QUANTIDADE,     0)) AS QTD_TOTAL_ITENS,
          SUM(COALESCE(i.OPD_QTD_PRODUZIDAS, 0)) AS QTD_PRODUZIDAS_ITENS,
          SUM(COALESCE(i.OPD_QTDE_SALDO,     0)) AS QTD_SALDO_ITENS
        FROM ORDEM_PRODUCAO_ITENS i
        WHERE i.OPD_ORP_SERIE = ?
        GROUP BY i.OPD_ORP_SERIE
    """, (orp_serie,))
    it = dict(zip(it_cols, it_rows[0])) if it_rows else {}

    # Cores
    cor_cols, cor_rows = safe_fetchall(cur, """
        SELECT
          i.OPD_ORP_SERIE AS ORP_SERIE,
          CAST(LIST(DISTINCT TRIM(c.COR_NOME), ', ') AS VARCHAR(200)) AS CORES_TXT
        FROM ORDEM_PRODUCAO_ITENS i
        LEFT JOIN PRODUTOS p
               ON p.PRO_CODIGO = i.OPD_PRO_CODIGO
        LEFT JOIN CORES c
               ON c.COR_CODIGO = COALESCE(i.OPD_COR_CODIGO, p.PRO_COR_CODIGO)
        WHERE i.OPD_ORP_SERIE = ?
          AND COALESCE(i.OPD_COR_CODIGO, p.PRO_COR_CODIGO) IS NOT NULL
        GROUP BY i.OPD_ORP_SERIE
    """, (orp_serie,))
    cor = cor_rows[0][cor_cols.index("CORES_TXT")] if cor_rows else "SEM PINTURA"

    # Percentual
    def num(x): return 0.0 if x is None else float(x)
    if it:
        tot = num(it.get("QTD_TOTAL_ITENS"))
        saldo = num(it.get("QTD_SALDO_ITENS"))
        pct = round((1 - (saldo / tot)) * 100, 2) if tot > 0 else 0.0
    else:
        tot = num(hdr_qtds.get("ORP_QTDE_PRODUCAO"))
        saldo = num(hdr_qtds.get("ORP_QTDE_SALDO"))
        pct = round((1 - (saldo / tot)) * 100, 2) if tot > 0 else 0.0

    print(f"\n=== Resumo (cor & % concluído) ===\nCOR={cor}; PERCENT_CONCLUIDO={pct}%")
    return cor, pct

def roteiro_da_op(cur, op_id: Any, orp_serie: Any):
    """
    Tenta detectar automaticamente tabela e colunas de ROTEIRO e listar as atividades.
    """
    info = resolve_roteiro_columns(cur)
    if not info:
        print("\n=== ROTEIRO ===\n(Não encontrei automaticamente a tabela/colunas do roteiro.)")
        return

    link_col = info["OP_NUM"]
    # se o nome da coluna de vínculo tiver "ID", usa op_id; caso contrário, usa orp_serie
    param = op_id if "ID" in link_col.upper() else orp_serie

    cols_select = [info["OP_NUM"], info["SETOR_COD"], info["SEQ"]]
    sql = f"""
        SELECT {', '.join(cols_select)}
        FROM {info['TABLE']}
        WHERE {info['OP_NUM']} = ?
        ORDER BY {info['SEQ']}
    """
    cols, rows = safe_fetchall(cur, sql, (param,))
    print_rows(f"ROTEIRO ({info['TABLE']})", cols, rows)

# ======================================================================================
# Main
# ======================================================================================

def main():
    con = connect_fb()
    cur = con.cursor()

    print("== Listando OPs recentes ==")
    ops = listar_ops_recentes(cur, limite=15)
    if not ops:
        print("Sem OPs para listar.")
        cur.close(); con.close(); return

    sugestao = str(ops[0].get("ORP_ID"))
    print("\nDigite um ORP_ID para detalhar (ENTER usa sugestão):", sugestao or "")
    try:
        op_id = input("> ").strip() or sugestao
    except EOFError:
        op_id = sugestao

    # Cabeçalho
    cab_cols, cab_rows = cabecalho_op(cur, op_id)
    if not cab_rows:
        print("OP não encontrada.")
        cur.close(); con.close(); return

    hdr = dict(zip(cab_cols, cab_rows[0]))
    orp_serie = hdr.get("ORP_SERIE")
    sts_txt = map_legacy_status(hdr.get("ORP_STS_CODIGO"))
    print(f"\nStatus textual: {sts_txt}")

    # Itens
    itens_da_op(cur, op_id, orp_serie)

    # Cor e percentual
    hdr_qtds = {
        "ORP_QTDE_PRODUCAO": hdr.get("ORP_QTDE_PRODUCAO"),
        "ORP_QTDE_PRODUZIDAS": hdr.get("ORP_QTDE_PRODUZIDAS"),
        "ORP_QTDE_SALDO": hdr.get("ORP_QTDE_SALDO"),
    }
    cor_e_percentual(cur, orp_serie, hdr_qtds)

    # Roteiro
    roteiro_da_op(cur, op_id, orp_serie)

    cur.close()
    con.close()
    print("\nConcluído (somente leitura).")

if __name__ == "__main__":
    main()
