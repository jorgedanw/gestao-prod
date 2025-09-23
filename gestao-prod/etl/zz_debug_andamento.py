import os, psycopg2
from dotenv import load_dotenv

BASE = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE, ".env"))

con = psycopg2.connect(
    host=os.getenv("PG_HOST","localhost"),
    port=int(os.getenv("PG_PORT","5432")),
    dbname=os.getenv("PG_DB","gp_local"),
    user=os.getenv("PG_USER","postgres"),
    password=os.getenv("PG_PASSWORD","")
)
cur = con.cursor()
cur.execute("SELECT COUNT(*) FROM andamento_setor")
print("linhas em andamento_setor:", cur.fetchone()[0])
cur.execute("SELECT DISTINCT setor_codigo FROM andamento_setor ORDER BY 1")
print("setores distintos:", [r[0] for r in cur.fetchall()])
cur.execute("""
  SELECT op_numero, setor_codigo, sequencia, status_setor, dt_inicio, dt_fim
  FROM andamento_setor ORDER BY op_numero DESC, sequencia LIMIT 20
""")
for r in cur.fetchall(): print(r)
cur.close(); con.close()
