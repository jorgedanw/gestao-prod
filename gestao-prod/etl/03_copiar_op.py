"""
03_copiar_op.py — Copia UMA OP do MSYSDADOS.FDB para o Postgres (upsert).
Uso:
  (.venv) PS> python .\etl\03_copiar_op.py 6456
"""
import os, sys
from typing import Tuple, List, Dict, Any, Optional
import psycopg2, psycopg2.extras
import firebirdsql
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(ENV_PATH)

# Firebird
FB_HOST = os.getenv("FIREBIRD_HOST", "localhost")
FB_PORT = int(os.getenv("FIREBIRD_PORT", "3050"))
FB_DB   = os.getenv("FIREBIRD_DB_PATH")
FB_USER = os.getenv("FIREBIRD_USER", "SYSDBA")
FB_PASS = os.getenv("FIREBIRD_PASSWORD", "masterkey")
FB_CHAR = os.getenv("FIREBIRD_CHARSET", "WIN1252")

# Postgres
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "gp_local")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASSWORD", "")

def fb_connect():
    return firebirdsql.connect(
        host=FB_HOST, port=FB_PORT, database=FB_DB,
        user=FB_USER, password=FB_PASS, charset=FB_CHAR
    )

def pg_connect():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS
    )

def fb_fetchone(cur, sql: str, params=()):
    cur.execute(sql, params); row = cur.fetchone()
    cols = [d[0] for d in cur.description]
    return cols, row

def fb_fetchall(cur, sql: str, params=()):
    cur.execute(sql, params); rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    return cols, rows

def map_status(code: Optional[str]) -> str:
    c = (code or "").strip().upper()
    if c == "AA": return "ABERTA"
    if c == "IN": return "INICIADA"
    if c in ("EP", "SS"): return "ENTRADA PARCIAL"
    if c == "FF": return "FINALIZADA"
    if c == "CC": return "CANCELADA"
    return "OUTRO"

def get_op_header(cur_fb, op_id: int) -> Dict[str, Any]:
    sql = """
        SELECT
          ORP_ID, ORP_SERIE, EMP_FIL_CODIGO, ORP_DESCRICAO, ORP_PDV_NUMERO,
          ORP_DATA, ORP_DT_PREV_INICIO, ORP_DT_VALIDADE,
          ORP_STS_CODIGO, ORP_STS_ID,
          ORP_QTDE_PRODUCAO, ORP_QTDE_PRODUZIDAS, ORP_QTDE_SALDO
        FROM ORDEM_PRODUCAO
        WHERE ORP_ID = ?
    """
    cols, row = fb_fetchone(cur_fb, sql, (op_id,))
    if not row:
        raise SystemExit(f"OP {op_id} não encontrada.")
    return dict(zip(cols, row))

def get_items(cur_fb, op_id: int, orp_serie: int) -> List[Dict[str, Any]]:
    base = "OPD_ID, OPD_ORP_ID, OPD_ORP_SERIE, OPD_LOTE, OPD_PRO_CODIGO, OPD_COR_CODIGO, OPD_QUANTIDADE, OPD_QTD_PRODUZIDAS, OPD_QTDE_SALDO"
    cols, rows = fb_fetchall(cur_fb, f"SELECT {base} FROM ORDEM_PRODUCAO_ITENS WHERE OPD_ORP_ID = ?", (op_id,))
    if not rows:
        cols, rows = fb_fetchall(cur_fb, f"SELECT {base} FROM ORDEM_PRODUCAO_ITENS WHERE OPD_ORP_SERIE = ?", (orp_serie,))
    return [dict(zip(cols, r)) for r in rows]

def get_color_and_percent(cur_fb, orp_serie: int, hdr: Dict[str, Any]) -> Tuple[str, float]:
    cols, row = fb_fetchone(cur_fb, """
        SELECT
          SUM(COALESCE(i.OPD_QUANTIDADE,     0)) AS QTD_TOTAL_ITENS,
          SUM(COALESCE(i.OPD_QTD_PRODUZIDAS, 0)) AS QTD_PRODUZIDAS_ITENS,
          SUM(COALESCE(i.OPD_QTDE_SALDO,     0)) AS QTD_SALDO_ITENS
        FROM ORDEM_PRODUCAO_ITENS i
        WHERE i.OPD_ORP_SERIE = ?
    """, (orp_serie,))
    it = dict(zip(cols, row)) if row else {}
    _, rowc = fb_fetchone(cur_fb, """
        SELECT CAST(LIST(DISTINCT TRIM(c.COR_NOME), ', ') AS VARCHAR(200))
        FROM ORDEM_PRODUCAO_ITENS i
        LEFT JOIN PRODUTOS p ON p.PRO_CODIGO = i.OPD_PRO_CODIGO
        LEFT JOIN CORES    c ON c.COR_CODIGO = COALESCE(i.OPD_COR_CODIGO, p.PRO_COR_CODIGO)
        WHERE i.OPD_ORP_SERIE = ?
          AND COALESCE(i.OPD_COR_CODIGO, p.PRO_COR_CODIGO) IS NOT NULL
    """, (orp_serie,))
    cor = rowc[0] if rowc and rowc[0] else "SEM PINTURA"

    def num(x): return 0.0 if x is None else float(x)
    if it and any(v is not None for v in it.values()):
        tot = num(it.get("QTD_TOTAL_ITENS")); saldo = num(it.get("QTD_SALDO_ITENS"))
        pct = round((1 - (saldo / tot)) * 100, 2) if tot > 0 else 0.0
    else:
        tot = num(hdr.get("ORP_QTDE_PRODUCAO")); saldo = num(hdr.get("ORP_QTDE_SALDO"))
        pct = round((1 - (saldo / tot)) * 100, 2) if tot > 0 else 0.0
    return cor, pct

def detect_roteiro(cur_fb) -> Optional[Dict[str,str]]:
    def list_user_tables():
        cur_fb.execute("""
          SELECT TRIM(r.rdb$relation_name)
          FROM rdb$relations r
          WHERE r.rdb$system_flag = 0 AND r.rdb$view_blr IS NULL
          ORDER BY 1
        """); return [r[0] for r in cur_fb.fetchall()]
    def list_columns(table):
        cur_fb.execute("""
          SELECT TRIM(rf.rdb$field_name)
          FROM rdb$relation_fields rf
          WHERE rf.rdb$relation_name = ?
          ORDER BY rf.rdb$field_position
        """, (table,)); return [r[0] for r in cur_fb.fetchall()]
    import re
    def pick(cols, pref, pats):
        U=[c.upper() for c in cols]
        for p in pref:
            if p.upper() in U: return p.upper()
        for rx in pats:
            for c in U:
                if rx.search(c): return c
        return None
    tables = list_user_tables()
    candidates = ["PCP_APTO_ROTEIRO","PCP_ORP_ROTEIRO","PCP_ROTEIRO","ROTEIRO"]
    for t in candidates:
        if t not in tables: continue
        cols = list_columns(t)
        opnum = pick(cols,
            ["OPR_ORP_NUMERO","APR_ORP_NUMERO","OPR_ORP_SERIE","APR_ORP_SERIE","ORP_NUMERO","ORP_SERIE","OPR_ORP_ID","APR_ORP_ID"],
            [re.compile(r"(^|_)ORP_?(NUM|NUMERO|SERIE|ID)$", re.I)]
        )
        setor = pick(cols,
            ["OPR_ATV_ID","APR_ATV_ID","OPR_SET_CODIGO","APR_SET_CODIGO","ATV_ID","ATV_CODIGO","SET_CODIGO","SETOR_CODIGO"],
            [re.compile(r"ATV.*(ID|COD)", re.I), re.compile(r"SET.*COD", re.I)]
        )
        seq   = pick(cols,
            ["OPR_ATV_SEQUENCIA","APR_ATV_SEQUENCIA","ATV_SEQUENCIA","SEQUENCIA","ORDEM","OPR_SEQ"],
            [re.compile(r"SEQ", re.I), re.compile(r"ORDEM", re.I)]
        )
        if opnum and setor and seq:
            return {"TABLE": t, "OP_NUM": opnum, "SETOR_COD": setor, "SEQ": seq}
    return None

def get_roteiro(cur_fb, op_id: int, orp_serie: int) -> List[Dict[str,Any]]:
    info = detect_roteiro(cur_fb)
    if not info: return []
    link = info["OP_NUM"].upper()
    param = op_id if "ID" in link else orp_serie
    cols, rows = fb_fetchall(cur_fb, f"""
        SELECT {info['OP_NUM']}, {info['SETOR_COD']}, {info['SEQ']}
        FROM {info['TABLE']}
        WHERE {info['OP_NUM']} = ?
        ORDER BY {info['SEQ']}
    """, (param,))
    return [dict(zip([c.upper() for c in cols], r)) for r in rows]

def ensure_schema(pg_cur):
    pg_cur.execute("""
    CREATE TABLE IF NOT EXISTS op (
      op_id           INTEGER PRIMARY KEY,
      op_numero       INTEGER UNIQUE,
      filial          INTEGER,
      descricao       TEXT,
      pedido_numero   INTEGER,
      status_code     VARCHAR(4),
      status_nome     VARCHAR(40),
      dt_emissao      TIMESTAMP,
      dt_prev_inicio  TIMESTAMP,
      dt_validade     TIMESTAMP,
      qtd_total_hdr       NUMERIC(18,3),
      qtd_produzidas_hdr  NUMERIC(18,3),
      qtd_saldo_hdr       NUMERIC(18,3),
      percent_concluido   NUMERIC(7,2),
      cor_txt         VARCHAR(200)
    );
    CREATE TABLE IF NOT EXISTS op_item (
      opd_id          INTEGER PRIMARY KEY,
      op_id           INTEGER REFERENCES op(op_id) ON DELETE CASCADE,
      op_numero       INTEGER,
      lote            INTEGER,
      pro_codigo      INTEGER,
      cor_codigo      INTEGER,
      qtd             NUMERIC(18,3),
      qtd_produzidas  NUMERIC(18,3),
      qtd_saldo       NUMERIC(18,3)
    );
    CREATE TABLE IF NOT EXISTS roteiro (
      id              BIGSERIAL PRIMARY KEY,
      op_numero       INTEGER NOT NULL,
      setor_codigo    INTEGER NOT NULL,
      sequencia       INTEGER NOT NULL,
      UNIQUE (op_numero, setor_codigo, sequencia)
    );
    """)

def upsert_op(pg_cur, op: Dict[str,Any]):
    pg_cur.execute("""
    INSERT INTO op (
      op_id, op_numero, filial, descricao, pedido_numero,
      status_code, status_nome, dt_emissao, dt_prev_inicio, dt_validade,
      qtd_total_hdr, qtd_produzidas_hdr, qtd_saldo_hdr, percent_concluido, cor_txt
    ) VALUES (
      %(ORP_ID)s, %(ORP_SERIE)s, %(EMP_FIL_CODIGO)s, %(ORP_DESCRICAO)s, %(ORP_PDV_NUMERO)s,
      %(status_code)s, %(status_nome)s, %(ORP_DATA)s, %(ORP_DT_PREV_INICIO)s, %(ORP_DT_VALIDADE)s,
      %(ORP_QTDE_PRODUCAO)s, %(ORP_QTDE_PRODUZIDAS)s, %(ORP_QTDE_SALDO)s, %(percent_concluido)s, %(cor_txt)s
    )
    ON CONFLICT (op_id) DO UPDATE SET
      op_numero = EXCLUDED.op_numero,
      filial = EXCLUDED.filial,
      descricao = EXCLUDED.descricao,
      pedido_numero = EXCLUDED.pedido_numero,
      status_code = EXCLUDED.status_code,
      status_nome = EXCLUDED.status_nome,
      dt_emissao = EXCLUDED.dt_emissao,
      dt_prev_inicio = EXCLUDED.dt_prev_inicio,
      dt_validade = EXCLUDED.dt_validade,
      qtd_total_hdr = EXCLUDED.qtd_total_hdr,
      qtd_produzidas_hdr = EXCLUDED.qtd_produzidas_hdr,
      qtd_saldo_hdr = EXCLUDED.qtd_saldo_hdr,
      percent_concluido = EXCLUDED.percent_concluido,
      cor_txt = EXCLUDED.cor_txt;
    """, op)

def upsert_items(pg_cur, items: List[Dict[str,Any]]):
    if not items: return
    psycopg2.extras.execute_batch(pg_cur, """
    INSERT INTO op_item (
      opd_id, op_id, op_numero, lote, pro_codigo, cor_codigo,
      qtd, qtd_produzidas, qtd_saldo
    ) VALUES (
      %(OPD_ID)s, %(OPD_ORP_ID)s, %(OPD_ORP_SERIE)s, %(OPD_LOTE)s, %(OPD_PRO_CODIGO)s, %(OPD_COR_CODIGO)s,
      %(OPD_QUANTIDADE)s, %(OPD_QTD_PRODUZIDAS)s, %(OPD_QTDE_SALDO)s
    )
    ON CONFLICT (opd_id) DO UPDATE SET
      op_id = EXCLUDED.op_id,
      op_numero = EXCLUDED.op_numero,
      lote = EXCLUDED.lote,
      pro_codigo = EXCLUDED.pro_codigo,
      cor_codigo = EXCLUDED.cor_codigo,
      qtd = EXCLUDED.qtd,
      qtd_produzidas = EXCLUDED.qtd_produzidas,
      qtd_saldo = EXCLUDED.qtd_saldo
    """, items, page_size=500)

def upsert_roteiro(pg_cur, op_numero: int, atividades: List[Dict[str,Any]]):
    if not atividades: return
    rows = []
    for a in atividades:
        setor = a.get("OPR_ATV_ID") or a.get("APR_ATV_ID") or a.get("OPR_SET_CODIGO") or a.get("APR_SET_CODIGO") or a.get("ATV_ID") or a.get("ATV_CODIGO")
        seq   = a.get("OPR_ATV_SEQUENCIA") or a.get("APR_ATV_SEQUENCIA") or a.get("ATV_SEQUENCIA") or a.get("SEQUENCIA") or a.get("ORDEM") or a.get("OPR_SEQ")
        if setor is None or seq is None: 
            continue
        rows.append({"op_numero": op_numero, "setor_codigo": int(setor), "sequencia": int(seq)})

    psycopg2.extras.execute_batch(pg_cur, """
    INSERT INTO roteiro (op_numero, setor_codigo, sequencia)
    VALUES (%(op_numero)s, %(setor_codigo)s, %(sequencia)s)
    ON CONFLICT (op_numero, setor_codigo, sequencia) DO NOTHING
    """, rows, page_size=500)

def main():
    if len(sys.argv) < 2:
        print("Uso: python 03_copiar_op.py <ORP_ID>")
        sys.exit(1)
    op_id = int(sys.argv[1])

    fb = fb_connect(); fbc = fb.cursor()
    pg = pg_connect(); pg.autocommit = False; pgc = pg.cursor()
    try:
        ensure_schema(pgc)

        hdr = get_op_header(fbc, op_id)
        orp_serie = hdr["ORP_SERIE"]
        cor_txt, percent = get_color_and_percent(fbc, orp_serie, hdr)

        hdr["status_code"] = hdr.get("ORP_STS_CODIGO")
        hdr["status_nome"] = map_status(hdr.get("ORP_STS_CODIGO"))
        hdr["percent_concluido"] = percent
        hdr["cor_txt"] = cor_txt

        items      = get_items(fbc, op_id, orp_serie)
        atividades = get_roteiro(fbc, op_id, orp_serie)

        upsert_op(pgc, hdr)
        upsert_items(pgc, items)
        upsert_roteiro(pgc, orp_serie, atividades)

        pg.commit()
        print(f"OK! OP {op_id} copiada/atualizada em {PG_DB}.")
    except Exception:
        pg.rollback()
        raise
    finally:
        fbc.close(); fb.close()
        pgc.close(); pg.close()

if __name__ == "__main__":
    main()
