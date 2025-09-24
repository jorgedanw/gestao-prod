# etl/04_copiar_janela.py
# -----------------------------------------------------------------------------
# Copia OPs do Firebird -> Postgres por janela de datas (emissão/prev_inicio/validade).
# - Leitura: Firebird (MSYSDADOS.FDB) via firebirdsql
# - Escrita: Postgres (tabelas op, op_item, roteiro)
# - Tolerante a variações de esquema no Firebird:
#     * Detecta dinamicamente a coluna de descrição em PRODUTOS
#     * Detecta dinamicamente a coluna de nome da cor em CORES
#     * SEM depender de colunas de cor em PRODUTOS (ex.: PRO_COR_CODIGO)
# -----------------------------------------------------------------------------
r"""
Como usar (PowerShell):

# Janela por PREVISÃO DE INÍCIO (últimos 7 até +21 dias)
(.venv) PS> python .\etl\04_copiar_janela.py --filial 1 --date-field prev_inicio --days-back 7 --days-ahead 21

# Janela por VALIDADE (recomendado no seu fluxo)
(.venv) PS> python .\etl\04_copiar_janela.py --filial 1 --date-field validade --days-back 7 --days-ahead 30

# Intervalo exato
(.venv) PS> python .\etl\04_copiar_janela.py --filial 1 --date-field validade --from 2025-05-01 --to 2025-10-30

# Apenas listar o que seria copiado (sem gravar)
(.venv) PS> python .\etl\04_copiar_janela.py --filial 1 --date-field validade --from 2025-05-01 --to 2025-10-30 --dry-run
"""

import os
import re  # << necessário para as detecções por regex
import sys
import argparse
from typing import Tuple, List, Dict, Any, Optional
from datetime import datetime, date, timedelta

import psycopg2
import psycopg2.extras
import firebirdsql
from dotenv import load_dotenv

# -----------------------------------------------------------------------------
# .env
# -----------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(ENV_PATH)

# Firebird (origem)
FB_HOST = os.getenv("FIREBIRD_HOST", "localhost")
FB_PORT = int(os.getenv("FIREBIRD_PORT", "3050"))
FB_DB   = os.getenv("FIREBIRD_DB_PATH")
FB_USER = os.getenv("FIREBIRD_USER", "SYSDBA")
FB_PASS = os.getenv("FIREBIRD_PASSWORD", "masterkey")
FB_CHAR = os.getenv("FIREBIRD_CHARSET", "WIN1252")

# Postgres (destino)
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "gp_local")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASSWORD", "")

# -----------------------------------------------------------------------------
# Conexões
# -----------------------------------------------------------------------------
def fb_connect():
    """Abre conexão com o Firebird."""
    if not FB_DB:
        raise SystemExit("Erro: defina FIREBIRD_DB_PATH em etl\\.env")
    return firebirdsql.connect(
        host=FB_HOST, port=FB_PORT, database=FB_DB,
        user=FB_USER, password=FB_PASS, charset=FB_CHAR
    )

def pg_connect():
    """Abre conexão com o Postgres."""
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS
    )

# -----------------------------------------------------------------------------
# Helpers Firebird (fetch)
# -----------------------------------------------------------------------------
def fb_fetchone(cur, sql: str, params=()):
    """Executa e retorna uma linha (com nomes de colunas)."""
    cur.execute(sql, params)
    row = cur.fetchone()
    cols = [d[0] for d in cur.description]
    return cols, row

def fb_fetchall(cur, sql: str, params=()):
    """Executa e retorna todas as linhas (com nomes de colunas)."""
    cur.execute(sql, params)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    return cols, rows

# -----------------------------------------------------------------------------
# Detecção de colunas no Firebird (tolerante a variações de esquema)
# -----------------------------------------------------------------------------
def fb_list_columns(cur_fb, table_name: str) -> List[str]:
    """
    Lista as colunas de uma tabela de usuário no Firebird (em UPPER).
    """
    cur_fb.execute("""
        SELECT TRIM(rf.rdb$field_name)
        FROM rdb$relation_fields rf
        WHERE rf.rdb$relation_name = ?
        ORDER BY rf.rdb$field_position
    """, (table_name.upper(),))
    return [r[0].upper() for r in cur_fb.fetchall()]

def fb_pick_column(cur_fb, table_name: str, regex_patterns: List[str]) -> str:
    """
    Dado um conjunto de regex, retorna o primeiro nome de coluna que bater.
    """
    cols = fb_list_columns(cur_fb, table_name)
    for pat in regex_patterns:
        rx = re.compile(pat, re.I)
        for c in cols:
            if rx.search(c):
                return c
    return ""

def fb_detect_product_desc_column(cur_fb) -> str:
    """
    Tenta achar a coluna de DESCRIÇÃO em PRODUTOS.
    Exemplos comuns: PRO_DESCRICAO, PRO_DESCR, DESCRICAO, DESCR.
    """
    return fb_pick_column(cur_fb, "PRODUTOS", [
        r"^PRO_?DESCR",   # PRO_DESCRICAO, PRO_DESCR...
        r"DESCR"          # qualquer coisa com DESCR
    ])

def fb_detect_color_name_column(cur_fb) -> str:
    """
    Tenta achar a coluna de NOME/DESCRIÇÃO em CORES.
    Exemplos comuns: COR_NOME, COR_DESCRICAO, NOME, DESCRICAO.
    """
    return fb_pick_column(cur_fb, "CORES", [
        r"^COR_?(NOME|DESCR)",  # COR_NOME, COR_DESCRICAO
        r"^(NOME|DESCR).*"      # NOME, DESCRICAO...
    ])

# -----------------------------------------------------------------------------
# Mapeamentos / schema Postgres
# -----------------------------------------------------------------------------
def map_status(code: Optional[str]) -> str:
    """Mapeia códigos do Microsys para nomes humanizados."""
    c = (code or "").strip().upper()
    if c == "AA": return "ABERTA"
    if c == "IN": return "INICIADA"
    if c in ("EP", "SS"): return "ENTRADA PARCIAL"
    if c == "FF": return "FINALIZADA"
    if c == "CC": return "CANCELADA"
    return "OUTRO"

def ensure_schema(pg_cur):
    """
    Cria tabelas-alvo no Postgres (se não existirem) e garante colunas.
    Obs.: a tabela cfg_pintura_prod fica no schema SQL principal (pg_schema.sql);
    não é necessária para o ETL funcionar, por isso não é criada aqui.
    """
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
      qtd_saldo       NUMERIC(18,3),
      pro_desc        TEXT,
      cor_nome        VARCHAR(200)
    );
    CREATE TABLE IF NOT EXISTS roteiro (
      id              BIGSERIAL PRIMARY KEY,
      op_numero       INTEGER NOT NULL,
      setor_codigo    INTEGER NOT NULL,
      sequencia       INTEGER NOT NULL,
      UNIQUE (op_numero, setor_codigo, sequencia)
    );
    """)
    # Migração suave (ambientes antigos)
    pg_cur.execute("ALTER TABLE op_item ADD COLUMN IF NOT EXISTS pro_desc TEXT;")
    pg_cur.execute("ALTER TABLE op_item ADD COLUMN IF NOT EXISTS cor_nome VARCHAR(200);")

def upsert_op(pg_cur, op: Dict[str,Any]):
    """UPSERT do cabeçalho da OP."""
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
    """UPSERT dos itens de OP."""
    if not items:
        return
    for it in items:
        it.setdefault("PRO_DESC", None)
        it.setdefault("COR_NOME", None)
    psycopg2.extras.execute_batch(pg_cur, """
    INSERT INTO op_item (
      opd_id, op_id, op_numero, lote, pro_codigo, cor_codigo,
      qtd, qtd_produzidas, qtd_saldo,
      pro_desc, cor_nome
    ) VALUES (
      %(OPD_ID)s, %(OPD_ORP_ID)s, %(OPD_ORP_SERIE)s, %(OPD_LOTE)s, %(OPD_PRO_CODIGO)s, %(OPD_COR_CODIGO)s,
      %(OPD_QUANTIDADE)s, %(OPD_QTD_PRODUZIDAS)s, %(OPD_QTDE_SALDO)s,
      %(PRO_DESC)s, %(COR_NOME)s
    )
    ON CONFLICT (opd_id) DO UPDATE SET
      op_id = EXCLUDED.op_id,
      op_numero = EXCLUDED.op_numero,
      lote = EXCLUDED.lote,
      pro_codigo = EXCLUDED.pro_codigo,
      cor_codigo = EXCLUDED.cor_codigo,
      qtd = EXCLUDED.qtd,
      qtd_produzidas = EXCLUDED.qtd_produzidas,
      qtd_saldo = EXCLUDED.qtd_saldo,
      pro_desc = EXCLUDED.pro_desc,
      cor_nome = EXCLUDED.cor_nome
    """, items, page_size=500)

def upsert_roteiro(pg_cur, op_numero: int, atividades: List[Dict[str,Any]]):
    """UPSERT do roteiro (setor/ordem)."""
    if not atividades:
        return
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

# -----------------------------------------------------------------------------
# Consultas Firebird
# -----------------------------------------------------------------------------
def get_op_header(cur_fb, op_id: int) -> Dict[str, Any]:
    """Lê cabeçalho da OP no Firebird."""
    cols, row = fb_fetchone(cur_fb, """
        SELECT
          ORP_ID, ORP_SERIE, EMP_FIL_CODIGO, ORP_DESCRICAO, ORP_PDV_NUMERO,
          ORP_DATA, ORP_DT_PREV_INICIO, ORP_DT_VALIDADE,
          ORP_STS_CODIGO, ORP_STS_ID,
          ORP_QTDE_PRODUCAO, ORP_QTDE_PRODUZIDAS, ORP_QTDE_SALDO
        FROM ORDEM_PRODUCAO
        WHERE ORP_ID = ?
    """, (op_id,))
    if not row:
        raise RuntimeError(f"OP {op_id} não encontrada.")
    return dict(zip(cols, row))

def get_items(cur_fb, op_id: int, orp_serie: int) -> List[Dict[str, Any]]:
    """
    Lê os itens da OP no Firebird. Traz:
      - código do produto (OPD_PRO_CODIGO)
      - descrição do produto (se conseguir detectar a coluna de PRODUTOS)
      - código e NOME da cor (apenas via i.OPD_COR_CODIGO + CORES)
      - quantidades (qtd, produzidas, saldo)
    """
    # Detecta colunas opcionais
    prod_desc_col  = fb_detect_product_desc_column(cur_fb)  # ex.: PRO_DESCRICAO
    color_name_col = fb_detect_color_name_column(cur_fb)    # ex.: COR_NOME

    # Exprs dinâmicas (se não achar, usa NULL)
    prod_desc_expr  = f"p.{prod_desc_col}" if prod_desc_col else "CAST(NULL AS VARCHAR(200))"
    color_name_expr = f"c.{color_name_col}" if color_name_col else "CAST(NULL AS VARCHAR(200))"

    # IMPORTANTE: não usamos nenhuma coluna de cor em PRODUTOS.
    # Usamos APENAS i.OPD_COR_CODIGO para linkar em CORES.
    base_sql = f"""
        SELECT
          i.OPD_ID,
          i.OPD_ORP_ID,
          i.OPD_ORP_SERIE,
          i.OPD_LOTE,
          i.OPD_PRO_CODIGO,
          {prod_desc_expr} AS PRO_DESC,
          i.OPD_COR_CODIGO AS OPD_COR_CODIGO,
          {color_name_expr} AS COR_NOME,
          i.OPD_QUANTIDADE,
          i.OPD_QTD_PRODUZIDAS,
          i.OPD_QTDE_SALDO
        FROM ORDEM_PRODUCAO_ITENS i
        LEFT JOIN PRODUTOS p ON p.PRO_CODIGO = i.OPD_PRO_CODIGO
        LEFT JOIN CORES    c ON c.COR_CODIGO = i.OPD_COR_CODIGO
        WHERE {{filtro}} = ?
        ORDER BY i.OPD_ID
    """
    # Tenta por OPD_ORP_ID; se vazio, tenta por ORP_SERIE
    cols, rows = fb_fetchall(cur_fb, base_sql.format(filtro="i.OPD_ORP_ID"), (op_id,))
    if not rows:
        cols, rows = fb_fetchall(cur_fb, base_sql.format(filtro="i.OPD_ORP_SERIE"), (orp_serie,))

    return [dict(zip([c.upper() for c in cols], r)) for r in rows]

def get_color_and_percent(cur_fb, orp_serie: int, hdr: Dict[str, Any]) -> Tuple[str, float]:
    """
    Calcula % concluído com base nas quantidades dos ITENS (prioritário) ou cabeçalho.
    Monta o texto de cor a partir dos nomes em CORES vinculados por i.OPD_COR_CODIGO.
    """
    # Totais por itens (para %)
    cols, row = fb_fetchone(cur_fb, """
        SELECT
          SUM(COALESCE(i.OPD_QUANTIDADE,     0)) AS QTD_TOTAL_ITENS,
          SUM(COALESCE(i.OPD_QTD_PRODUZIDAS, 0)) AS QTD_PRODUZIDAS_ITENS,
          SUM(COALESCE(i.OPD_QTDE_SALDO,     0)) AS QTD_SALDO_ITENS
        FROM ORDEM_PRODUCAO_ITENS i
        WHERE i.OPD_ORP_SERIE = ?
    """, (orp_serie,))
    it = dict(zip(cols, row)) if row else {}

    # Nome da cor (detectado) em CORES
    color_name_col = fb_detect_color_name_column(cur_fb)  # ex.: COR_NOME
    color_expr = f"TRIM(c.{color_name_col})" if color_name_col else "NULL"

    # Lista de cores distintas vinculadas aos ITENS desta série
    _, rowc = fb_fetchone(cur_fb, f"""
        SELECT CAST(LIST(DISTINCT {color_expr}, ', ') AS VARCHAR(200))
        FROM ORDEM_PRODUCAO_ITENS i
        LEFT JOIN CORES c ON c.COR_CODIGO = i.OPD_COR_CODIGO
        WHERE i.OPD_ORP_SERIE = ?
          AND i.OPD_COR_CODIGO IS NOT NULL
    """, (orp_serie,))
    cor = rowc[0] if rowc and rowc[0] else "SEM PINTURA"

    # % concluído
    def num(x): return 0.0 if x is None else float(x)
    if it and any(v is not None for v in it.values()):
        tot = num(it.get("QTD_TOTAL_ITENS")); saldo = num(it.get("QTD_SALDO_ITENS"))
        pct = round((1 - (saldo / tot)) * 100, 2) if tot > 0 else 0.0
    else:
        tot = num(hdr.get("ORP_QTDE_PRODUCAO")); saldo = num(hdr.get("ORP_QTDE_SALDO"))
        pct = round((1 - (saldo / tot)) * 100, 2) if tot > 0 else 0.0

    return cor, pct

def detect_roteiro(cur_fb) -> Optional[Dict[str,str]]:
    """
    Detecta qual tabela/colunas representam o roteiro no seu Firebird.
    """
    def list_user_tables():
        cur_fb.execute("""
          SELECT TRIM(r.rdb$relation_name)
          FROM rdb$relations r
          WHERE r.rdb$system_flag = 0 AND r.rdb$view_blr IS NULL
          ORDER BY 1
        """)
        return [r[0] for r in cur_fb.fetchall()]

    def list_columns(table):
        cur_fb.execute("""
          SELECT TRIM(rf.rdb$field_name)
          FROM rdb$relation_fields rf
          WHERE rf.rdb$relation_name = ?
          ORDER BY rf.rdb$field_position
        """, (table,))
        return [r[0] for r in cur_fb.fetchall()]

    tables = list_user_tables()
    candidates = ["PCP_APTO_ROTEIRO","PCP_ORP_ROTEIRO","PCP_ROTEIRO","ROTEIRO"]

    # Heurística leve
    for t in candidates:
        if t not in tables:
            continue
        cols = [c.upper() for c in list_columns(t)]
        # Op num/id
        opnum = next((c for c in cols if re.search(r"(^|_)ORP_?(NUM|NUMERO|SERIE|ID)$", c, re.I)), None)
        # Setor
        setor = next((c for c in cols if re.search(r"(OPR_)?(ATV|SET).*?(ID|COD)", c, re.I)), None)
        # Sequência
        seq = next((c for c in cols if re.search(r"(SEQ|ORDEM)", c, re.I)), None)
        if opnum and setor and seq:
            return {"TABLE": t, "OP_NUM": opnum, "SETOR_COD": setor, "SEQ": seq}
    return None

def get_roteiro(cur_fb, op_id: int, orp_serie: int) -> List[Dict[str,Any]]:
    """
    Lê o roteiro (setores/ordem) da OP, independente do nome real da tabela.
    """
    info = detect_roteiro(cur_fb)
    if not info:
        return []
    link = info["OP_NUM"].upper()
    param = op_id if "ID" in link else orp_serie

    cols, rows = fb_fetchall(cur_fb, f"""
        SELECT {info['OP_NUM']}, {info['SETOR_COD']}, {info['SEQ']}
        FROM {info['TABLE']}
        WHERE {info['OP_NUM']} = ?
        ORDER BY {info['SEQ']}
    """, (param,))
    return [dict(zip([c.upper() for c in cols], r)) for r in rows]

# -----------------------------------------------------------------------------
# Seleção de OPs por janela (Firebird)
# -----------------------------------------------------------------------------
def find_ops_window(cur_fb, filial: int, status_list: List[str],
                    date_field: str, dt_from: date, dt_to: date,
                    limit: Optional[int] = None) -> List[int]:
    """
    Localiza ORP_IDs no Firebird dentro da janela/filial/status.
    """
    field_map = {
        "validade":   "ORP_DT_VALIDADE",
        "prev_inicio":"ORP_DT_PREV_INICIO",
        "emissao":    "ORP_DATA",
    }
    col = field_map.get(date_field, "ORP_DT_VALIDADE")
    status_tuple = tuple([s.strip().upper() for s in status_list if s.strip()])

    sql = f"""
        SELECT { 'FIRST ' + str(limit) if limit else '' } ORP_ID
        FROM ORDEM_PRODUCAO op
        WHERE op.EMP_FIL_CODIGO = ?
          AND COALESCE(op.ORP_FECHADO, 0) = 0
          AND COALESCE(op.ORP_STS_CODIGO, '') IN ({','.join(['?']*len(status_tuple))})
          AND {col} BETWEEN ? AND ?
        ORDER BY {col} NULLS LAST, op.ORP_SERIE DESC
    """
    params = [filial] + list(status_tuple) + [dt_from, dt_to]
    _, rows = fb_fetchall(cur_fb, sql, params)
    return [int(r[0]) for r in rows]

# -----------------------------------------------------------------------------
# Worker de cópia
# -----------------------------------------------------------------------------
def copy_one_op(fbc, pgc, op_id: int) -> bool:
    """
    Copia 1 OP do Firebird p/ Postgres (cabeçalho, itens, roteiro).
    """
    try:
        # Cabeçalho e metadados calculados
        hdr = get_op_header(fbc, op_id)
        orp_serie = hdr["ORP_SERIE"]

        # Calcula cor e % concluído de forma robusta
        cor_txt, percent = get_color_and_percent(fbc, orp_serie, hdr)

        hdr["status_code"] = hdr.get("ORP_STS_CODIGO")
        hdr["status_nome"] = map_status(hdr.get("ORP_STS_CODIGO"))
        hdr["percent_concluido"] = percent
        hdr["cor_txt"] = cor_txt

        # Itens e roteiro
        items      = get_items(fbc, op_id, orp_serie)
        atividades = get_roteiro(fbc, op_id, orp_serie)

        # UPSERT no Postgres
        upsert_op(pgc, hdr)
        upsert_items(pgc, items)
        upsert_roteiro(pgc, orp_serie, atividades)
        return True

    except Exception as e:
        print(f"[ERRO] OP {op_id}: {e}")
        return False

# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def parse_args():
    ap = argparse.ArgumentParser(description="Copia OPs por janela do Firebird para Postgres.")
    ap.add_argument("--filial", type=int, required=True, help="Código da filial (EMP_FIL_CODIGO).")
    ap.add_argument("--date-field", choices=["prev_inicio","validade","emissao"], default="validade",
                    help="Campo de data para filtrar a janela.")
    ap.add_argument("--status", type=str, default="AA,IN,EP,SS",
                    help="Lista de status (ex.: 'AA,IN,EP,SS').")
    ap.add_argument("--from", dest="dt_from", type=str, help="Data inicial (YYYY-MM-DD).")
    ap.add_argument("--to",   dest="dt_to",   type=str, help="Data final (YYYY-MM-DD).")
    ap.add_argument("--days-back", type=int, default=7,  help="Dias para trás (se --from/--to não informados).")
    ap.add_argument("--days-ahead", type=int, default=30, help="Dias para frente (se --from/--to não informados).")
    ap.add_argument("--limit", type=int, default=None, help="Limita a quantidade de OPs.")
    ap.add_argument("--dry-run", action="store_true", help="Mostra as OPs que seriam copiadas, sem gravar.")
    return ap.parse_args()

def main():
    args = parse_args()

    # Janela
    today = date.today()
    if args.dt_from and args.dt_to:
        dt_from = datetime.strptime(args.dt_from, "%Y-%m-%d").date()
        dt_to   = datetime.strptime(args.dt_to,   "%Y-%m-%d").date()
    else:
        dt_from = today - timedelta(days=args.days_back)
        dt_to   = today + timedelta(days=args.days_ahead)

    status_list = args.status.split(",")

    # Conexões
    fb = fb_connect(); fbc = fb.cursor()
    try:
        # Seleciona OPs na janela
        op_ids = find_ops_window(fbc, args.filial, status_list, args.date_field, dt_from, dt_to, args.limit)
        if not op_ids:
            print(f"Nenhuma OP encontrada para filial={args.filial}, campo={args.date_field}, janela={dt_from}..{dt_to}, status={status_list}")
            return

        print(f"Encontradas {len(op_ids)} OP(s): {op_ids[:10]}{' ...' if len(op_ids)>10 else ''}")
        if args.dry_run:
            print("DRY-RUN: nada será gravado no Postgres.")
            return

        pg = pg_connect(); pg.autocommit = False; pgc = pg.cursor()
        try:
            ensure_schema(pgc)
            ok = 0; fail = 0
            for opid in op_ids:
                if copy_one_op(fbc, pgc, opid):
                    ok += 1
                else:
                    fail += 1
            pg.commit()
            print(f"Concluído. Sucesso: {ok}; Falhas: {fail}.")
        except Exception:
            pg.rollback()
            raise
        finally:
            pgc.close(); pg.close()
    finally:
        fbc.close(); fb.close()

if __name__ == "__main__":
    main()
