# backend/app.py
# -----------------------------------------------------------------------------
# API somente leitura do GP
# - Correção: painel "Por Cor" do /dashboard agora usa a MESMA regra de cor
#   aplicada nas listas: se o.cor_txt for nulo/vazio/"SEM PINTURA", caímos
#   para as cores vindas dos itens (op_item.cor_nome) ou, por fim, para as
#   observações dos códigos cadastrados em cfg_pintura_prod.
# - Nenhuma outra rota/configuração foi alterada.
# -----------------------------------------------------------------------------

from fastapi import FastAPI, Query, HTTPException, Body  # Body só para as novas rotas
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from typing import List, Optional, Any
import os, psycopg2, psycopg2.extras
from datetime import date, timedelta, datetime
from dotenv import load_dotenv

# ------------------------------------------------------------
# Carrega .env (tenta backend/.env e depois etl/.env)
# ------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
for p in (os.path.join(BASE_DIR, ".env"),
          os.path.join(os.path.dirname(BASE_DIR), "etl", ".env")):
    if os.path.exists(p):
        load_dotenv(p)   # mantém o primeiro encontrado
        break

# Força encoding de cliente do Postgres em UTF-8 (robustez)
os.environ.setdefault("PGCLIENTENCODING", "UTF8")

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "gp_local")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASSWORD", "")

def get_conn():
    # DSN + options garante client_encoding=UTF8
    dsn = (
        f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} "
        f"user={PG_USER} password={PG_PASS} application_name=gp_api"
    )
    return psycopg2.connect(dsn=dsn, options='-c client_encoding=UTF8')

# Nomes de setores (para exibir no detalhe)
SETOR_LEGACY_MAP = {1: "Perfiladeira", 3: "Serralheria", 4: "Pintura", 6: "Eixo"}

app = FastAPI(title="GP - API de OPs (somente leitura)", version="0.6.2")

# CORS liberado (útil para servir o front pelo Live Server/VSCode em 5500)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"], allow_credentials=False,
)

@app.get("/health")
def health():
    return {"ok": True, "db": PG_DB}

def _parse_window(from_str: Optional[str], to_str: Optional[str], days_back: int, days_ahead: int):
    """Converte a janela (from/to) ou usa days_back/days_ahead a partir de hoje."""
    today = date.today()
    if from_str and to_str:
        dt_from = datetime.strptime(from_str, "%Y-%m-%d").date()
        dt_to   = datetime.strptime(to_str,   "%Y-%m-%d").date()
    else:
        dt_from = today - timedelta(days=days_back)
        dt_to   = today + timedelta(days=days_ahead)
    return dt_from, dt_to

# ============================================================================
# /ops — listagem com filtros, paginação e ordenação
#   + agrega m² de pintura
#   + compõe cor_txt final com CASE (tratando "SEM PINTURA" como vazio)
# ============================================================================
@app.get("/ops")
def list_ops(
    filial: int = Query(..., description="EMP_FIL_CODIGO"),
    date_field: str = Query("validade", regex="^(validade|prev_inicio|emissao)$"),
    status: str = Query("ABERTA,INICIADA,ENTRADA PARCIAL"),
    from_date: Optional[str] = Query(None, alias="from"),
    to_date:   Optional[str] = Query(None, alias="to"),
    days_back: int = 7,
    days_ahead:int = 30,
    q: Optional[str] = Query(None),
    cor_contains: Optional[str] = Query(None),
    percent_min: Optional[float] = None,
    percent_max: Optional[float] = None,
    page: int = 1,
    page_size: int = 50,
    order_by: str = Query("validade", regex="^(validade|prev_inicio|emissao|percent|op_numero)$"),
    order_dir: str = Query("desc", regex="^(asc|desc)$"),
):
    dt_from, dt_to = _parse_window(from_date, to_date, days_back, days_ahead)
    field_map = {"validade":"dt_validade", "prev_inicio":"dt_prev_inicio", "emissao":"dt_emissao"}
    order_map = {"validade":"dt_validade","prev_inicio":"dt_prev_inicio","emissao":"dt_emissao","percent":"percent_concluido","op_numero":"op_numero"}
    col = field_map[date_field]
    order_col = order_map[order_by]
    status_list = [s.strip().upper() for s in status.split(",") if s.strip()]
    offset = max(page-1, 0) * max(page_size, 1)

    where = ["o.filial = %s", f"o.{col} BETWEEN %s AND %s", "o.status_nome = ANY(%s)"]
    params: List[Any] = [filial, dt_from, dt_to, status_list]

    if q:
        where.append("(o.descricao ILIKE %s OR CAST(o.op_numero AS TEXT) ILIKE %s OR CAST(o.pedido_numero AS TEXT) ILIKE %s)")
        like = f"%{q}%"
        params += [like, like, like]
    if percent_min is not None:
        where.append("o.percent_concluido >= %s")
        params.append(percent_min)
    if percent_max is not None:
        where.append("o.percent_concluido <= %s")
        params.append(percent_max)

    where_sql = " AND ".join(where)
    order_sql = f"{order_col} {'ASC' if order_dir=='asc' else 'DESC'} NULLS LAST, o.op_numero DESC"

    # Expressão única da cor final, para SELECT e para filtro por cor
    cor_expr = """
    CASE
      WHEN o.cor_txt IS NULL OR BTRIM(o.cor_txt) = '' OR UPPER(BTRIM(o.cor_txt)) = 'SEM PINTURA'
        THEN COALESCE(NULLIF(cores.cores_dist,''), NULLIF(cfgcores.cores_cfg,''), 'SEM PINTURA')
      ELSE o.cor_txt
    END
    """

    sql_count = f"SELECT COUNT(*) FROM op o WHERE {where_sql}"

    sql_page = f"""
      WITH paint AS (
        SELECT
          i.op_id,
          COALESCE(SUM(i.qtd), 0)              AS m2_pintura_total,
          COALESCE(SUM(i.qtd_produzidas), 0)   AS m2_pintura_produzida,
          COALESCE(SUM(i.qtd_saldo), 0)        AS m2_pintura_saldo
        FROM op_item i
        JOIN cfg_pintura_prod cfg ON cfg.pro_codigo = i.pro_codigo
        GROUP BY i.op_id
      ),
      cores AS (
        SELECT
          i.op_id,
          STRING_AGG(DISTINCT TRIM(i.cor_nome), ', ' ORDER BY TRIM(i.cor_nome)) AS cores_dist
        FROM op_item i
        WHERE i.cor_nome IS NOT NULL AND BTRIM(i.cor_nome) <> ''
        GROUP BY i.op_id
      ),
      cfgcores AS (
        SELECT
          i.op_id,
          STRING_AGG(DISTINCT TRIM(cfg.observacao), ', ' ORDER BY TRIM(cfg.observacao)) AS cores_cfg
        FROM op_item i
        JOIN cfg_pintura_prod cfg ON cfg.pro_codigo = i.pro_codigo
        GROUP BY i.op_id
      )
      SELECT
        o.op_id, o.op_numero, o.filial, o.descricao, o.pedido_numero,
        o.status_code, o.status_nome, o.dt_emissao, o.dt_prev_inicio, o.dt_validade,
        o.percent_concluido,
        {cor_expr} AS cor_txt,
        COALESCE(p.m2_pintura_total, 0)      AS m2_pintura_total,
        COALESCE(p.m2_pintura_produzida, 0)  AS m2_pintura_produzida,
        COALESCE(p.m2_pintura_saldo, 0)      AS m2_pintura_saldo
      FROM op o
      LEFT JOIN paint    p  ON p.op_id = o.op_id
      LEFT JOIN cores       ON cores.op_id = o.op_id
      LEFT JOIN cfgcores    ON cfgcores.op_id = o.op_id
      WHERE {where_sql}
      {"AND (" + cor_expr + ") ILIKE %s" if cor_contains else ""}
      ORDER BY {order_sql}
      LIMIT %s OFFSET %s
    """

    params_page = list(params)
    if cor_contains:
        params_page.append(f"%{cor_contains}%")
    params_page += [page_size, offset]

    with get_conn() as con, con.cursor() as cur:
        cur.execute(sql_count, params)
        total = cur.fetchone()[0]

    with get_conn() as con, con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql_page, params_page)
        rows = cur.fetchall()
        return JSONResponse(content=jsonable_encoder({
            "total": total, "page": page, "page_size": page_size,
            "window": {"from": str(dt_from), "to": str(dt_to), "field": date_field},
            "items": rows
        }))

# ============================================================================
# /ops/faltando-pintura — OPs onde falta somente Pintura
#   + compõe cor final com CASE (tratando "SEM PINTURA")
#   + devolve m² de pintura
# ============================================================================
@app.get("/ops/faltando-pintura")
def ops_faltando_pintura(
    filial: int = Query(...),
    date_field: str = Query("validade", regex="^(validade|prev_inicio|emissao)$"),
    from_date: Optional[str] = Query(None, alias="from"),
    to_date:   Optional[str] = Query(None, alias="to"),
    days_back: int = 7,
    days_ahead:int = 30,
    status: str = Query("ABERTA,INICIADA,ENTRADA PARCIAL"),
    limit: int = 200
):
    dt_from, dt_to = _parse_window(from_date, to_date, days_back, days_ahead)
    field_map = {"validade":"dt_validade", "prev_inicio":"dt_prev_inicio", "emissao":"dt_emissao"}
    col = field_map[date_field]
    status_list = [s.strip().upper() for s in status.split(",") if s.strip()]

    pintura_like = [
        "%TINTA%", "%PINT%", "%EPOX%", "%EPOXI%", "%EPOXY%",
        "%PRIMER%", "%ELETRO%", "%PU%", "%ESMALTE%"
    ]

    # Expressão única da cor final (para SELECT)
    cor_expr_b = """
    CASE
      WHEN b.cor_txt IS NULL OR BTRIM(b.cor_txt) = '' OR UPPER(BTRIM(b.cor_txt)) = 'SEM PINTURA'
        THEN COALESCE(NULLIF(cores.cores_dist,''), NULLIF(cfgcores.cores_cfg,''), 'SEM PINTURA')
      ELSE b.cor_txt
    END
    """

    sql_main = f"""
    WITH base AS (
      SELECT
        o.op_id, o.op_numero, o.descricao, o.status_nome, o.percent_concluido,
        o.cor_txt, o.{col} AS ref_data
      FROM op o
      WHERE o.filial = %s
        AND o.status_nome = ANY(%s)
        AND o.{col} BETWEEN %s AND %s
    ),
    itens AS (
      SELECT
        i.op_id, i.op_numero, i.opd_id, i.pro_codigo, i.pro_desc, i.cor_nome,
        COALESCE(i.qtd, 0)              AS qtd,
        COALESCE(i.qtd_produzidas, 0)   AS qtd_produzidas,
        COALESCE(i.qtd_saldo, 0)        AS qtd_saldo,
        CASE WHEN cfg.pro_codigo IS NOT NULL THEN TRUE
             WHEN i.pro_desc ILIKE ANY(%s) THEN TRUE
             ELSE FALSE
        END AS is_paint_by_cfg_desc,
        CASE WHEN i.cor_nome IS NOT NULL AND i.pro_desc ILIKE ANY(%s) THEN TRUE ELSE FALSE END AS is_paint_by_color
      FROM op_item i
      LEFT JOIN cfg_pintura_prod cfg ON cfg.pro_codigo = i.pro_codigo
    ),
    marcados AS (
      SELECT
        it.op_id, it.op_numero, it.opd_id, it.qtd, it.qtd_produzidas, it.qtd_saldo,
        it.pro_codigo, it.pro_desc, it.cor_nome,
        (it.is_paint_by_cfg_desc OR it.is_paint_by_color) AS is_paint
      FROM itens it
    ),
    agregado AS (
      SELECT
        m.op_id,
        SUM(CASE WHEN NOT m.is_paint THEN m.qtd_saldo ELSE 0 END) AS saldo_nao_pint,
        SUM(CASE WHEN m.is_paint     THEN m.qtd_saldo ELSE 0 END) AS saldo_pint,
        COUNT(*) FILTER (WHERE m.is_paint) AS itens_pint,
        COUNT(*) FILTER (WHERE NOT m.is_paint) AS itens_nao_pint,
        SUM(CASE WHEN m.is_paint THEN m.qtd            ELSE 0 END) AS m2_pintura_total,
        SUM(CASE WHEN m.is_paint THEN m.qtd_produzidas ELSE 0 END) AS m2_pintura_produzida,
        SUM(CASE WHEN m.is_paint THEN m.qtd_saldo      ELSE 0 END) AS m2_pintura_saldo
      FROM marcados m
      GROUP BY m.op_id
    ),
    cores AS (
      SELECT op_id, STRING_AGG(DISTINCT TRIM(cor_nome), ', ' ORDER BY TRIM(cor_nome)) AS cores_dist
      FROM itens
      WHERE cor_nome IS NOT NULL AND BTRIM(cor_nome) <> ''
      GROUP BY op_id
    ),
    cfgcores AS (
      SELECT
        it.op_id,
        STRING_AGG(DISTINCT TRIM(cfg.observacao), ', ' ORDER BY TRIM(cfg.observacao)) AS cores_cfg
      FROM itens it
      JOIN cfg_pintura_prod cfg ON cfg.pro_codigo = it.pro_codigo
      GROUP BY it.op_id
    )
    SELECT
      b.op_id, b.op_numero, b.descricao, b.status_nome, b.percent_concluido,
      {cor_expr_b} AS cor_txt,
      b.ref_data AS {col},
      a.m2_pintura_total,
      a.m2_pintura_produzida,
      a.m2_pintura_saldo
    FROM base b
    JOIN agregado a  ON a.op_id  = b.op_id
    LEFT JOIN cores  ON cores.op_id  = b.op_id
    LEFT JOIN cfgcores ON cfgcores.op_id = b.op_id
    WHERE a.itens_pint > 0
      AND a.saldo_nao_pint = 0
      AND a.saldo_pint    > 0
    ORDER BY b.ref_data NULLS LAST, b.op_numero DESC
    LIMIT %s
    """

    # Fallback sem cfg_pintura_prod
    cor_expr_b_fallback = """
    CASE
      WHEN b.cor_txt IS NULL OR BTRIM(b.cor_txt) = '' OR UPPER(BTRIM(b.cor_txt)) = 'SEM PINTURA'
        THEN COALESCE(NULLIF(cores.cores_dist,''), 'SEM PINTURA')
      ELSE b.cor_txt
    END
    """

    sql_fallback = f"""
    WITH base AS (
      SELECT
        o.op_id, o.op_numero, o.descricao, o.status_nome, o.percent_concluido,
        o.cor_txt, o.{col} AS ref_data
      FROM op o
      WHERE o.filial = %s
        AND o.status_nome = ANY(%s)
        AND o.{col} BETWEEN %s AND %s
    ),
    itens AS (
      SELECT
        i.op_id, i.op_numero, i.opd_id, i.pro_codigo, i.pro_desc, i.cor_nome,
        COALESCE(i.qtd, 0)              AS qtd,
        COALESCE(i.qtd_produzidas, 0)   AS qtd_produzidas,
        COALESCE(i.qtd_saldo, 0)        AS qtd_saldo,
        CASE WHEN i.pro_desc ILIKE ANY(%s) THEN TRUE ELSE FALSE END AS is_paint_by_cfg_desc,
        CASE WHEN i.cor_nome IS NOT NULL AND i.pro_desc ILIKE ANY(%s) THEN TRUE ELSE FALSE END AS is_paint_by_color
      FROM op_item i
    ),
    marcados AS (
      SELECT
        it.op_id, it.op_numero, it.opd_id, it.qtd, it.qtd_produzidas, it.qtd_saldo,
        it.pro_codigo, it.pro_desc, it.cor_nome,
        (it.is_paint_by_cfg_desc OR it.is_paint_by_color) AS is_paint
      FROM itens it
    ),
    agregado AS (
      SELECT
        m.op_id,
        SUM(CASE WHEN NOT m.is_paint THEN m.qtd_saldo ELSE 0 END) AS saldo_nao_pint,
        SUM(CASE WHEN m.is_paint     THEN m.qtd_saldo ELSE 0 END) AS saldo_pint,
        COUNT(*) FILTER (WHERE m.is_paint) AS itens_pint,
        COUNT(*) FILTER (WHERE NOT m.is_paint) AS itens_nao_pint,
        SUM(CASE WHEN m.is_paint THEN m.qtd            ELSE 0 END) AS m2_pintura_total,
        SUM(CASE WHEN m.is_paint THEN m.qtd_produzidas ELSE 0 END) AS m2_pintura_produzida,
        SUM(CASE WHEN m.is_paint THEN m.qtd_saldo      ELSE 0 END) AS m2_pintura_saldo
      FROM marcados m
      GROUP BY m.op_id
    ),
    cores AS (
      SELECT op_id, STRING_AGG(DISTINCT TRIM(cor_nome), ', ' ORDER BY TRIM(cor_nome)) AS cores_dist
      FROM itens
      WHERE cor_nome IS NOT NULL AND BTRIM(cor_nome) <> ''
      GROUP BY op_id
    )
    SELECT
      b.op_id, b.op_numero, b.descricao, b.status_nome, b.percent_concluido,
      {cor_expr_b_fallback} AS cor_txt,
      b.ref_data AS {col},
      a.m2_pintura_total,
      a.m2_pintura_produzida,
      a.m2_pintura_saldo
    FROM base b
    JOIN agregado a ON a.op_id = b.op_id
    LEFT JOIN cores ON cores.op_id = b.op_id
    WHERE a.itens_pint > 0
      AND a.saldo_nao_pint = 0
      AND a.saldo_pint    > 0
    ORDER BY b.ref_data NULLS LAST, b.op_numero DESC
    LIMIT %s
    """

    params_common = [filial, status_list, dt_from, dt_to, pintura_like, pintura_like, limit]

    with get_conn() as con, con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        try:
            cur.execute(sql_main, params_common)
            rows = cur.fetchall()
            mode = "itens_heuristica+cfg"
        except psycopg2.errors.UndefinedTable:
            con.rollback()
            cur.execute(sql_fallback, [filial, status_list, dt_from, dt_to, pintura_like, pintura_like, limit])
            rows = cur.fetchall()
            mode = "itens_heuristica"

    payload = {
        "count": len(rows),
        "items": rows,
        "mode": mode,
        "heuristica": pintura_like,
        "window": {"from": str(dt_from), "to": str(dt_to), "field": date_field}
    }
    return JSONResponse(content=jsonable_encoder(payload))

# ============================================================================
# /ops/{op_id} — Detalhe (+ m² de pintura) com cor_txt final corrigida
# ============================================================================
@app.get("/ops/{op_id}")
def get_op(op_id: int):
    with get_conn() as con, con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # Cabeçalho
        cur.execute("""
            SELECT op_id, op_numero, filial, descricao, pedido_numero,
                   status_code, status_nome, dt_emissao, dt_prev_inicio, dt_validade,
                   percent_concluido, cor_txt,
                   qtd_total_hdr, qtd_produzidas_hdr, qtd_saldo_hdr
            FROM op WHERE op_id = %s
        """, (op_id,))
        op = cur.fetchone()
        if not op:
            raise HTTPException(404, detail="OP não encontrada")

        # Cor final (o.cor_txt vs itens.cor_nome vs cfg.observacao)
        cur.execute("""
            SELECT
            CASE
              WHEN o.cor_txt IS NULL OR BTRIM(o.cor_txt) = '' OR UPPER(BTRIM(o.cor_txt)) = 'SEM PINTURA'
                THEN COALESCE(
                       NULLIF((
                          SELECT STRING_AGG(DISTINCT TRIM(i.cor_nome), ', ' ORDER BY TRIM(i.cor_nome))
                          FROM op_item i
                          WHERE i.op_id = o.op_id
                            AND i.cor_nome IS NOT NULL
                            AND BTRIM(i.cor_nome) <> ''
                       ), ''),
                       NULLIF((
                          SELECT STRING_AGG(DISTINCT TRIM(cfg.observacao), ', ' ORDER BY TRIM(cfg.observacao))
                          FROM op_item i
                          JOIN cfg_pintura_prod cfg ON cfg.pro_codigo = i.pro_codigo
                          WHERE i.op_id = o.op_id
                       ), ''),
                       'SEM PINTURA'
                     )
              ELSE o.cor_txt
            END AS cor_txt_final
            FROM op o
            WHERE o.op_id = %s
        """, (op_id,))
        row_cor = cur.fetchone()
        if row_cor and row_cor.get("cor_txt_final"):
            op["cor_txt"] = row_cor["cor_txt_final"]

        # Resumo de itens
        cur.execute("""
            SELECT COUNT(*) AS itens, 
                   COALESCE(SUM(qtd),0) AS qtd_total,
                   COALESCE(SUM(qtd_saldo),0) AS qtd_saldo,
                   COALESCE(SUM(qtd_produzidas),0) AS qtd_produzidas
            FROM op_item WHERE op_id = %s
        """, (op_id,))
        resumo = cur.fetchone()

        # Itens
        cur.execute("""
            SELECT opd_id, lote, pro_codigo, pro_desc, cor_codigo, cor_nome, qtd, qtd_produzidas, qtd_saldo
            FROM op_item WHERE op_id = %s ORDER BY opd_id
        """, (op_id,))
        itens = cur.fetchall()

        # Roteiro
        cur.execute("""
            SELECT setor_codigo, sequencia
            FROM roteiro WHERE op_numero = %s ORDER BY sequencia, setor_codigo
        """, (op["op_numero"],))
        rot = cur.fetchall()
        for r in rot:
            r["setor_nome"] = SETOR_LEGACY_MAP.get(r["setor_codigo"])

        # m² de pintura (se cfg existir)
        try:
            cur.execute("""
                SELECT
                  COALESCE(SUM(i.qtd), 0)            AS m2_pintura_total,
                  COALESCE(SUM(i.qtd_produzidas), 0) AS m2_pintura_produzida,
                  COALESCE(SUM(i.qtd_saldo), 0)      AS m2_pintura_saldo
                FROM op_item i
                JOIN cfg_pintura_prod cfg ON cfg.pro_codigo = i.pro_codigo
                WHERE i.op_id = %s
            """, (op_id,))
            m2_row = cur.fetchone() or {}
        except psycopg2.errors.UndefinedTable:
            con.rollback()
            m2_row = {"m2_pintura_total": 0.0, "m2_pintura_produzida": 0.0, "m2_pintura_saldo": 0.0}

        return JSONResponse(content=jsonable_encoder({
            "op": op,
            "resumo_itens": resumo,
            "itens": itens,
            "roteiro": rot,
            "pintura_m2": {
                "total": float(m2_row.get("m2_pintura_total") or 0),
                "produzida": float(m2_row.get("m2_pintura_produzida") or 0),
                "saldo": float(m2_row.get("m2_pintura_saldo") or 0),
            }
        }))

# ============================================================================
# /dashboard — agregados para gráficos
#   *ALTERAÇÃO*: "Por Cor" usa a mesma regra de cor final das outras rotas.
# ============================================================================
@app.get("/dashboard")
def dashboard(
    filial: int = Query(...),
    date_field: str = Query("validade", regex="^(validade|prev_inicio|emissao)$"),
    from_date: Optional[str] = Query(None, alias="from"),
    to_date:   Optional[str] = Query(None, alias="to"),
    days_back: int = 7,
    days_ahead:int = 30,
    status: str = Query("ABERTA,INICIADA,ENTRADA PARCIAL"),
):
    dt_from, dt_to = _parse_window(from_date, to_date, days_back, days_ahead)
    field_map = {"validade":"dt_validade", "prev_inicio":"dt_prev_inicio", "emissao":"dt_emissao"}
    col = field_map[date_field]
    status_list = [s.strip().upper() for s in status.split(",") if s.strip()]
    where = f"o.filial = %s AND o.{col} BETWEEN %s AND %s AND o.status_nome = ANY(%s)"
    params = [filial, dt_from, dt_to, status_list]

    with get_conn() as con, con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # Por Status
        cur.execute(f"""
          SELECT o.status_nome, COUNT(*) AS qtd
          FROM op o
          WHERE {where}
          GROUP BY o.status_nome
          ORDER BY qtd DESC
        """, params)
        by_status = cur.fetchall()

        # Por Cor (mesma lógica de cor final)
        cor_expr = """
        CASE
          WHEN o.cor_txt IS NULL OR BTRIM(o.cor_txt) = '' OR UPPER(BTRIM(o.cor_txt)) = 'SEM PINTURA'
            THEN COALESCE(NULLIF(cores.cores_dist,''), NULLIF(cfgcores.cores_cfg,''), 'SEM PINTURA')
          ELSE o.cor_txt
        END
        """

        sql_by_color = f"""
        WITH
        cores AS (
          SELECT i.op_id,
                 STRING_AGG(DISTINCT TRIM(i.cor_nome), ', ' ORDER BY TRIM(i.cor_nome)) AS cores_dist
          FROM op_item i
          WHERE i.cor_nome IS NOT NULL AND BTRIM(i.cor_nome) <> ''
          GROUP BY i.op_id
        ),
        cfgcores AS (
          SELECT i.op_id,
                 STRING_AGG(DISTINCT TRIM(cfg.observacao), ', ' ORDER BY TRIM(cfg.observacao)) AS cores_cfg
          FROM op_item i
          JOIN cfg_pintura_prod cfg ON cfg.pro_codigo = i.pro_codigo
          GROUP BY i.op_id
        ),
        base AS (
          SELECT
            {cor_expr} AS cor_final
          FROM op o
          LEFT JOIN cores    ON cores.op_id    = o.op_id
          LEFT JOIN cfgcores ON cfgcores.op_id = o.op_id
          WHERE {where}
        )
        SELECT cor_final AS cor, COUNT(*) AS qtd
        FROM base
        GROUP BY cor_final
        ORDER BY qtd DESC
        """
        cur.execute(sql_by_color, params)
        by_color = cur.fetchall()

        # Série diária
        cur.execute(f"""
          SELECT date_trunc('day', o.{col})::date AS dia, COUNT(*) AS qtd
          FROM op o
          WHERE {where}
          GROUP BY dia ORDER BY dia
        """, params)
        series = cur.fetchall()

        # Média %
        cur.execute(f"""
          SELECT ROUND(AVG(o.percent_concluido)::numeric, 2) AS media_percent
          FROM op o
          WHERE {where}
        """, params)
        avg_percent = (cur.fetchone() or {}).get("media_percent")

    return JSONResponse(content=jsonable_encoder({
        "window": {"from": str(dt_from), "to": str(dt_to), "field": date_field},
        "by_status": by_status,
        "by_color": by_color,
        "series": series,
        "avg_percent_concluido": float(avg_percent) if avg_percent is not None else None
    }))

# ============================================================
# 🔵 MÓDULO ADICIONAL: Operações da Pintura (Operador)
#     - novas rotas; não toca no que já existe
#     - usa tabelas app_setor_exec / app_event (ver DDL no seu ETL)
# ============================================================

PINTURA_SETOR = 4  # código do setor Pintura

def _exec_row_to_dict(row):
    if not row:
        return None
    return {
        "op_numero": row[0],
        "setor_codigo": row[1],
        "status_setor": row[2],
        "dt_inicio": row[3].isoformat() if row[3] else None,
        "dt_fim": row[4].isoformat() if row[4] else None,
        "usuario": row[5],
        "obs": row[6],
    }

def _log_event(con, op_numero:int, setor:int, evt:str, usuario:str=None, payload:dict=None):
    with con.cursor() as cur:
        cur.execute("""
            INSERT INTO app_event (op_numero, setor_codigo, event, usuario, payload)
            VALUES (%s,%s,%s,%s,%s)
        """, (op_numero, setor, evt, usuario, psycopg2.extras.Json(payload or {})))

@app.post("/operacoes/pintura/iniciar")
def iniciar_pintura(body: dict = Body(...)):
    """Inicia a Pintura de uma OP: body = { "op_numero": 6102, "usuario": "NOME" }"""
    op_numero = int(body.get("op_numero"))
    usuario   = (body.get("usuario") or "").strip() or None

    with get_conn() as con, con.cursor() as cur:
        cur.execute("""
            INSERT INTO app_setor_exec (op_numero, setor_codigo, status_setor, dt_inicio, dt_fim, usuario, obs)
            VALUES (%s, %s, 'EM_EXECUCAO', now(), NULL, %s, NULL)
            ON CONFLICT (op_numero, setor_codigo) DO UPDATE
            SET status_setor = 'EM_EXECUCAO',
                dt_inicio    = COALESCE(app_setor_exec.dt_inicio, EXCLUDED.dt_inicio),
                dt_fim       = NULL,
                usuario      = EXCLUDED.usuario
            RETURNING op_numero, setor_codigo, status_setor, dt_inicio, dt_fim, usuario, obs
        """, (op_numero, PINTURA_SETOR, usuario))
        row = cur.fetchone()
        _log_event(con, op_numero, PINTURA_SETOR, "INICIAR_PINTURA", usuario, {"from":"api"})

    return {"ok": True, "exec": _exec_row_to_dict(row)}

@app.post("/operacoes/pintura/finalizar")
def finalizar_pintura(body: dict = Body(...)):
    """Finaliza a Pintura de uma OP: body = { "op_numero": 6102, "usuario": "NOME", "obs": "OK" }"""
    op_numero = int(body.get("op_numero"))
    usuario   = (body.get("usuario") or "").strip() or None
    obs       = (body.get("obs") or "").strip() or None

    with get_conn() as con, con.cursor() as cur:
        cur.execute("""
            INSERT INTO app_setor_exec (op_numero, setor_codigo, status_setor, dt_inicio, dt_fim, usuario, obs)
            VALUES (%s, %s, 'CONCLUIDO', now(), now(), %s, %s)
            ON CONFLICT (op_numero, setor_codigo) DO UPDATE
            SET status_setor = 'CONCLUIDO',
                dt_inicio    = COALESCE(app_setor_exec.dt_inicio, app_setor_exec.dt_inicio),
                dt_fim       = now(),
                usuario      = COALESCE(%s, app_setor_exec.usuario),
                obs          = COALESCE(%s, app_setor_exec.obs)
            RETURNING op_numero, setor_codigo, status_setor, dt_inicio, dt_fim, usuario, obs
        """, (op_numero, PINTURA_SETOR, usuario, obs, usuario, obs))
        row = cur.fetchone()
        _log_event(con, op_numero, PINTURA_SETOR, "FINALIZAR_PINTURA", usuario, {"from":"api","obs":obs})

    return {"ok": True, "exec": _exec_row_to_dict(row)}

@app.get("/operacoes/pintura/status")
def status_pintura(op_numero: int = Query(..., description="Número da OP")):
    """Retorna o status local da Pintura para esta OP (se já iniciada/finalizada via app)."""
    with get_conn() as con, con.cursor() as cur:
        cur.execute("""
            SELECT op_numero, setor_codigo, status_setor, dt_inicio, dt_fim, usuario, obs
            FROM app_setor_exec
            WHERE op_numero = %s AND setor_codigo = %s
        """, (op_numero, PINTURA_SETOR))
        row = cur.fetchone()
        return {"ok": True, "exec": _exec_row_to_dict(row)}

@app.get("/pintura/fila")
def pintura_fila(
    filial: int = Query(...),
    date_field: str = Query("validade", regex="^(validade|prev_inicio|emissao)$"),
    from_date: Optional[str] = Query(None, alias="from"),
    to_date:   Optional[str] = Query(None, alias="to"),
    days_back: int = 7,
    days_ahead:int = 30,
    status: str = Query("ABERTA,INICIADA,ENTRADA PARCIAL"),
    limit: int = 300
):
    """
    Fila de OPs que faltam apenas a etapa de PINTURA (mesma lógica do /ops/faltando-pintura)
    + overlay do status local do operador (app_setor_exec).
    """
    dt_from, dt_to = _parse_window(from_date, to_date, days_back, days_ahead)
    field_map = {"validade":"dt_validade", "prev_inicio":"dt_prev_inicio", "emissao":"dt_emissao"}
    col = field_map[date_field]
    status_list = [s.strip().upper() for s in status.split(",") if s.strip()]

    pintura_like = [
        "%TINTA%", "%PINT%", "%EPOX%", "%EPOXI%", "%EPOXY%",
        "%PRIMER%", "%ELETRO%", "%PU%", "%ESMALTE%"
    ]

    sql_main = f"""
    WITH base AS (
      SELECT o.op_id, o.op_numero, o.descricao, o.status_nome, o.percent_concluido,
             o.cor_txt, o.{col} AS ref_data
      FROM op o
      WHERE o.filial = %s
        AND o.status_nome = ANY(%s)
        AND o.{col} BETWEEN %s AND %s
    ),
    itens AS (
      SELECT i.op_id, i.op_numero, i.opd_id, i.pro_codigo, i.pro_desc, i.cor_nome,
             COALESCE(i.qtd_saldo,0) AS qtd_saldo,
             CASE WHEN cfg.pro_codigo IS NOT NULL THEN TRUE
                  WHEN i.pro_desc ILIKE ANY(%s) THEN TRUE
                  ELSE FALSE END AS is_paint_by_cfg_desc,
             CASE WHEN i.cor_nome IS NOT NULL AND i.pro_desc ILIKE ANY(%s) THEN TRUE ELSE FALSE END AS is_paint_by_color
      FROM op_item i
      LEFT JOIN cfg_pintura_prod cfg ON cfg.pro_codigo = i.pro_codigo
    ),
    marcados AS (
      SELECT it.op_id, it.op_numero, it.opd_id, it.qtd_saldo, it.pro_codigo, it.pro_desc, it.cor_nome,
             (it.is_paint_by_cfg_desc OR it.is_paint_by_color) AS is_paint
      FROM itens it
    ),
    agregado AS (
      SELECT m.op_id,
             SUM(CASE WHEN NOT m.is_paint THEN m.qtd_saldo ELSE 0 END) AS saldo_nao_pint,
             SUM(CASE WHEN     m.is_paint THEN m.qtd_saldo ELSE 0 END) AS saldo_pint,
             COUNT(*) FILTER (WHERE m.is_paint)     AS itens_pint,
             COUNT(*) FILTER (WHERE NOT m.is_paint) AS itens_nao_pint
      FROM marcados m
      GROUP BY m.op_id
    ),
    cores AS (
      SELECT op_id, STRING_AGG(DISTINCT TRIM(cor_nome), ', ' ORDER BY TRIM(cor_nome)) AS cores_dist
      FROM itens
      WHERE cor_nome IS NOT NULL
      GROUP BY op_id
    )
    SELECT
      b.op_id,
      b.op_numero,
      b.descricao,
      b.status_nome,
      b.percent_concluido,
      COALESCE(cores.cores_dist, b.cor_txt) AS cor_txt,
      b.ref_data AS {col},
      x.status_setor AS exec_status,
      x.dt_inicio   AS exec_dt_inicio,
      x.dt_fim      AS exec_dt_fim,
      x.usuario     AS exec_usuario
    FROM base b
    JOIN agregado a ON a.op_id = b.op_id
    LEFT JOIN cores ON cores.op_id = b.op_id
    LEFT JOIN app_setor_exec x ON x.op_numero = b.op_numero AND x.setor_codigo = {PINTURA_SETOR}
    WHERE a.itens_pint > 0
      AND a.saldo_nao_pint = 0
      AND a.saldo_pint    > 0
    ORDER BY b.{col} NULLS LAST, b.op_numero DESC
    LIMIT %s
    """

    params = [filial, status_list, dt_from, dt_to, pintura_like, pintura_like, limit]
    try:
        with get_conn() as con, con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql_main, params)
            rows = cur.fetchall()
            return {
                "count": len(rows),
                "items": rows,
                "window": {"from": str(dt_from), "to": str(dt_to), "field": date_field},
                "mode": "fila_pintura+exec"
            }
    except psycopg2.errors.UndefinedTable:
        # se não existir cfg_pintura_prod, mesma consulta sem o JOIN na cfg
        with get_conn() as con, con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            sql_fb = sql_main.replace("LEFT JOIN cfg_pintura_prod cfg ON cfg.pro_codigo = i.pro_codigo", "")
            cur.execute(sql_fb, params)
            rows = cur.fetchall()
            return {
                "count": len(rows),
                "items": rows,
                "window": {"from": str(dt_from), "to": str(dt_to), "field": date_field},
                "mode": "fila_pintura+exec (sem cfg)"
            }
