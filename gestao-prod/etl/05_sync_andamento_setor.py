"""
05_sync_andamento_setor.py — Sincroniza andamento por setor a partir do Firebird para Postgres.
Classifica cada etapa em: PENDENTE | EM_EXECUCAO | CONCLUIDO
Usa qualquer tabela ROTEIRO detectada (PCP_APTO_ROTEIRO, PCP_ORP_ROTEIRO, PCP_ROTEIRO, ROTEIRO).

Exemplos:
  python .\etl\05_sync_andamento_setor.py --days-back 7 --days-ahead 30
  python .\etl\05_sync_andamento_setor.py --from 2025-09-01 --to 2025-10-31
"""
import os, argparse, re
from datetime import date, datetime, timedelta
from typing import Optional, Dict, Any, List
import firebirdsql, psycopg2, psycopg2.extras
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

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
    return firebirdsql.connect(host=FB_HOST, port=FB_PORT, database=FB_DB, user=FB_USER, password=FB_PASS, charset=FB_CHAR)

def pg_connect():
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS)

def list_user_tables(cur):
    cur.execute("""
      SELECT TRIM(r.rdb$relation_name)
      FROM rdb$relations r
      WHERE r.rdb$system_flag = 0 AND r.rdb$view_blr IS NULL
      ORDER BY 1
    """); return [r[0] for r in cur.fetchall()]

def list_columns(cur, table: str):
    cur.execute("""
      SELECT TRIM(rf.rdb$field_name)
      FROM rdb$relation_fields rf
      WHERE rf.rdb$relation_name = ?
      ORDER BY rf.rdb$field_position
    """,(table,)); return [r[0] for r in cur.fetchall()]

def pick(cols: List[str], preferred: List[str], patterns: List[re.Pattern]) -> Optional[str]:
    U=[c.upper() for c in cols]
    for p in preferred:
        if p.upper() in U: return p.upper()
    for rx in patterns:
        for c in U:
            if rx.search(c): return c
    return None

def detect_roteiro(cur) -> Optional[Dict[str,str]]:
    tabs = list_user_tables(cur)
    candidates = ["PCP_APTO_ROTEIRO","PCP_ORP_ROTEIRO","PCP_ROTEIRO","ROTEIRO"]
    for t in candidates:
        if t not in tabs: continue
        cols = list_columns(cur, t)
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
        start = pick(cols,
            ["OPR_ATV_DT_INICIO","APR_ATV_DT_INICIO","DT_INICIO","DATA_INICIO","OPR_DT_INICIO","APR_DT_INICIO","INICIO","DTINI"],
            [re.compile(r"INI(CIO)?", re.I)]
        )
        end   = pick(cols,
            ["OPR_ATV_DT_FIM","APR_ATV_DT_FIM","DT_FIM","DATA_FIM","OPR_DT_FIM","APR_DT_FIM","FINAL","CONCLUSAO","DTFIM"],
            [re.compile(r"(FIM|FINAL|CONCL)", re.I)]
        )
        stat  = pick(cols,
            ["OPR_ATV_STATUS","APR_ATV_STATUS","OPR_STATUS","APR_STATUS","STATUS","SITUACAO","IND_REALIZACAO","OPR_IND_REALIZACAO"],
            [re.compile(r"STATUS|SITU|IND", re.I)]
        )
        if opnum and setor and seq:
            return {"TABLE": t, "OP_NUM": opnum, "SETOR": setor, "SEQ": seq, "DTINI": start, "DTFIM": end, "STATUS": stat}
    return None

def derive_stage_status(dtini, dtfim, statval) -> str:
    if dtfim: return "CONCLUIDO"
    if dtini: return "EM_EXECUCAO"
    if statval:
        s = str(statval).strip().upper()
        if s in ("FF","FINALIZADO","FINALIZADA","CONCLUIDO","CONCLUIDA","FECHADO","FECHADA","F","C","2"):
            return "CONCLUIDO"
        if s in ("IN","INICIADO","INICIADA","EXECUCAO","EXECUTANDO","ANDAMENTO","A","1"):
            return "EM_EXECUCAO"
    return "PENDENTE"

def ensure_schema_pg(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS andamento_setor (
      op_numero    INTEGER NOT NULL,
      setor_codigo INTEGER NOT NULL,
      sequencia    INTEGER NOT NULL,
      status_setor VARCHAR(20) NOT NULL,
      dt_inicio    TIMESTAMP NULL,
      dt_fim       TIMESTAMP NULL,
      PRIMARY KEY (op_numero, setor_codigo, sequencia)
    );
    CREATE INDEX IF NOT EXISTS idx_andamento_op ON andamento_setor(op_numero);
    """)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="dt_from", type=str)
    ap.add_argument("--to", dest="dt_to", type=str)
    ap.add_argument("--days-back", type=int, default=7)
    ap.add_argument("--days-ahead", type=int, default=30)
    args = ap.parse_args()

    today = date.today()
    if args.dt_from and args.dt_to:
        dt_from = datetime.strptime(args.dt_from, "%Y-%m-%d").date()
        dt_to   = datetime.strptime(args.dt_to,   "%Y-%m-%d").date()
    else:
        dt_from = today - timedelta(days=args.days_back)
        dt_to   = today + timedelta(days=args.days_ahead)

    fb = fb_connect(); fbc = fb.cursor()
    pg = pg_connect(); pg.autocommit=False; pgc = pg.cursor()

    try:
        ensure_schema_pg(pgc)
        pgc.execute("""SELECT DISTINCT op_numero FROM op WHERE dt_validade BETWEEN %s AND %s""", (dt_from, dt_to))
        ops = [r[0] for r in pgc.fetchall()]
        if not ops:
            print(f"Nenhuma OP no Postgres em {dt_from}..{dt_to}. Rode 04_copiar_janela primeiro.")
            pg.rollback(); return

        info = detect_roteiro(fbc)
        if not info:
            raise SystemExit("Não foi possível detectar a tabela de roteiro no Firebird.")

        sel = [info["OP_NUM"], info["SETOR"], info["SEQ"]]
        if info["DTINI"]: sel.append(info["DTINI"])
        if info["DTFIM"]: sel.append(info["DTFIM"])
        if info["STATUS"]: sel.append(info["STATUS"])

        sql = f"SELECT {', '.join(sel)} FROM {info['TABLE']} WHERE {info['OP_NUM']} = ? ORDER BY {info['SEQ']}"

        rows_to_upsert = []
        for opn in ops:
            fbc.execute(sql, (opn,))
            cols = [d[0].upper() for d in fbc.description]
            for r in fbc.fetchall():
                rec = dict(zip(cols, r))
                setor = rec.get(info["SETOR"].upper())
                seq   = rec.get(info["SEQ"].upper())
                dtini = rec.get((info["DTINI"] or "").upper()) if info["DTINI"] else None
                dtfim = rec.get((info["DTFIM"] or "").upper()) if info["DTFIM"] else None
                statv = rec.get((info["STATUS"] or "").upper()) if info["STATUS"] else None
                status_setor = derive_stage_status(dtini, dtfim, statv)
                if setor is None or seq is None: 
                    continue
                rows_to_upsert.append({
                    "op_numero": int(opn),
                    "setor_codigo": int(setor),
                    "sequencia": int(seq),
                    "status_setor": status_setor,
                    "dt_inicio": dtini,
                    "dt_fim": dtfim
                })

        if rows_to_upsert:
            psycopg2.extras.execute_batch(pgc, """
            INSERT INTO andamento_setor (op_numero, setor_codigo, sequencia, status_setor, dt_inicio, dt_fim)
            VALUES (%(op_numero)s, %(setor_codigo)s, %(sequencia)s, %(status_setor)s, %(dt_inicio)s, %(dt_fim)s)
            ON CONFLICT (op_numero, setor_codigo, sequencia) DO UPDATE SET
              status_setor = EXCLUDED.status_setor,
              dt_inicio = EXCLUDED.dt_inicio,
              dt_fim = EXCLUDED.dt_fim
            """, rows_to_upsert, page_size=1000)
            pg.commit()
            print(f"Sincronizado andamento_setor: {len(rows_to_upsert)} linhas.")
        else:
            print("Nada para sincronizar.")

    finally:
        fbc.close(); fb.close()
        pgc.close(); pg.close()

if __name__ == "__main__":
    main()
