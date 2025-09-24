# etl/03_check_pg.py
# Verifica as últimas OPs gravadas no Postgres gp_local.
import os
import psycopg2
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

con = psycopg2.connect(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", "5432")),
    dbname=os.getenv("PG_DB", "gp_local"),
    user=os.getenv("PG_USER", "postgres"),
    password=os.getenv("PG_PASSWORD", "")
)
cur = con.cursor()
cur.execute("""
    SELECT op_id, op_numero, status_nome, percent_concluido, cor_txt
    FROM op
    ORDER BY op_id DESC
    LIMIT 10
""")
rows = cur.fetchall()
if not rows:
    print("Sem dados na tabela op (rode 03_copiar_op.py primeiro).")
else:
    for r in rows:
        print(r)
cur.close()
con.close()
