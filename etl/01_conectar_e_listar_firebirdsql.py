"""
PARTE 1 (Plano B) — Conectar ao Firebird usando driver 100% Python (firebirdsql)
Somente leitura. Usa variáveis em etl\.env
"""

import os
import firebirdsql
from dotenv import load_dotenv

# Descobre a pasta deste script (etl/) e carrega o .env dela
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(ENV_PATH)

FB_HOST = os.getenv("FIREBIRD_HOST", "localhost")          # ex.: SRVFERROSUL
FB_PORT = int(os.getenv("FIREBIRD_PORT", "3050"))          # 3050 é o padrão
FB_DB   = os.getenv("FIREBIRD_DB_PATH")                    # ex.: C:\Microsys\...\MSYSDADOS.FDB (visto pelo servidor) ou alias
FB_USER = os.getenv("FIREBIRD_USER", "SYSDBA")
FB_PASS = os.getenv("FIREBIRD_PASSWORD", "masterkey")
FB_CHAR = os.getenv("FIREBIRD_CHARSET", "WIN1252")         # comum em instalações Microsys

if not FB_DB:
    raise SystemExit("Erro: defina FIREBIRD_DB_PATH em etl\\.env")

print(f"Conectando via firebirdsql em {FB_HOST}:{FB_PORT}:{FB_DB} ...")
con = firebirdsql.connect(
    host=FB_HOST,
    port=FB_PORT,
    database=FB_DB,   # ex.: 'SRVFERROSUL:C:\\Microsys\\MsysIndustrial\\Dados\\MSYSDADOS.FDB' também funciona se for DSN completo
    user=FB_USER,
    password=FB_PASS,
    charset=FB_CHAR
)
print("OK! Conexão estabelecida (somente leitura).")

cur = con.cursor()

# Lista TABELAS de usuário (ignora views e objetos do sistema)
cur.execute("""
    SELECT TRIM(r.rdb$relation_name)
    FROM rdb$relations r
    WHERE r.rdb$system_flag = 0
      AND r.rdb$view_blr IS NULL
    ORDER BY 1
""")
tabelas = [row[0] for row in cur.fetchall()]
print(f"Total de tabelas de usuário: {len(tabelas)}")
for t in tabelas[:20]:
    print(" -", t)

# Amostra opcional de uma tabela conhecida
candidatos = ["ORDEM_PRODUCAO", "PCP_ORP_ROTEIRO", "PEDIDOS_VENDA", "CLIENTES"]
for nome in candidatos:
    if nome in tabelas:
        print(f"\nAmostra de {nome}:")
        cur.execute(f"SELECT FIRST 2 * FROM {nome}")
        cols = [d[0] for d in cur.description]
        print("Colunas:", cols)
        for r in cur.fetchall():
            print("Linha:", r)
        break

cur.close()
con.close()
print("\nConcluído. Nada foi modificado no MSYSDADOS.FDB.")
