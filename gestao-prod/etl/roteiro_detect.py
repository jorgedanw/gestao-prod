# etl/roteiro_detect.py
import re
from typing import List, Optional, Dict, Tuple

def list_user_tables(cur) -> List[str]:
    cur.execute("""
        SELECT TRIM(r.rdb$relation_name)
        FROM rdb$relations r
        WHERE r.rdb$system_flag = 0
          AND r.rdb$view_blr IS NULL
        ORDER BY 1
    """)
    return [row[0] for row in cur.fetchall()]

def list_columns(cur, table: str) -> List[str]:
    cur.execute("""
        SELECT TRIM(rf.rdb$field_name)
        FROM rdb$relation_fields rf
        WHERE rf.rdb$relation_name = ?
        ORDER BY rf.rdb$field_position
    """, (table,))
    return [row[0] for row in cur.fetchall()]

def pick_col(cols: List[str], candidates: List[str], patterns: List[re.Pattern]) -> Optional[str]:
    upp = [c.upper() for c in cols]
    # 1) tenta nomes exatos preferidos
    for cand in candidates:
        u = cand.upper()
        if u in upp:
            return u
    # 2) tenta por regex/padrão
    for rx in patterns:
        for c in upp:
            if rx.search(c):
                return c
    return None

def resolve_roteiro_columns(cur) -> Optional[Dict[str,str]]:
    """
    Descobre TABELA e COLUNAS do roteiro nesta base:
      - OP_NUM: coluna que liga ao número/série da OP
      - SETOR_COD: coluna do código do setor
      - SEQ: coluna de ordem/sequência
    Retorna dict com {TABLE, OP_NUM, SETOR_COD, SEQ} ou None se não achar.
    """
    # Candidatas comuns em Microsys
    table_candidates = [
        "PCP_APTO_ROTEIRO",
        "PCP_ORP_ROTEIRO",
        "PCP_ROTEIRO",
        "ROTEIRO",
    ]
    cols_patterns = {
        "op_num_candidates": [
            "OPR_ORP_NUMERO","APR_ORP_NUMERO","OPR_ORP_SERIE","APR_ORP_SERIE","ORP_NUMERO","ORP_SERIE",
        ],
        "op_num_regex": [re.compile(r"(^|_)ORP_?(NUM|NUMERO|SERIE)$", re.I), re.compile(r"(ORDEM|OP)_?PROD", re.I), re.compile(r"ORP", re.I)],
        # Na sua base, setor costuma vir como OPR_ATV_ID (ou variações)
        "setor_candidates": [
            "OPR_ATV_ID","APR_ATV_ID","OPR_SET_CODIGO","APR_SET_CODIGO","ATV_ID","ATV_CODIGO","SET_CODIGO","SETOR_CODIGO",
        ],
        "setor_regex": [re.compile(r"ATV.*(ID|COD)", re.I), re.compile(r"SET.*COD", re.I), re.compile(r"SETOR", re.I)],
        "seq_candidates": ["OPR_ATV_SEQUENCIA","APR_ATV_SEQUENCIA","ATV_SEQUENCIA","SEQUENCIA","ORDEM","OPR_SEQ"],
        "seq_regex": [re.compile(r"SEQ", re.I), re.compile(r"ORDEM", re.I)],
    }

    tabs = list_user_tables(cur)
    for t in table_candidates:
        if t not in tabs:
            continue
        cols = list_columns(cur, t)
        op_num   = pick_col(cols, cols_patterns["op_num_candidates"], cols_patterns["op_num_regex"])
        setor    = pick_col(cols, cols_patterns["setor_candidates"], cols_patterns["setor_regex"])
        seq      = pick_col(cols, cols_patterns["seq_candidates"], cols_patterns["seq_regex"])
        if op_num and setor and seq:
            return {"TABLE": t, "OP_NUM": op_num, "SETOR_COD": setor, "SEQ": seq}
    return None
