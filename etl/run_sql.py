# etl/run_sql.py
# ----------------------------------------------------------
# Executa um arquivo .sql no Postgres e imprime os resultados.
# Uso:
#   (.venv) PS> python .\etl\run_sql.py .\etl\sql\quick_check.sql
# ----------------------------------------------------------
import os, sys, textwrap
import psycopg2, psycopg2.extras
from dotenv import load_dotenv

# Tenta carregar variáveis de ambiente (primeiro etl/.env, depois backend/.env)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
for p in (os.path.join(BASE_DIR, ".env"),
          os.path.join(os.path.dirname(BASE_DIR), "backend", ".env")):
    if os.path.exists(p):
        load_dotenv(p)
        break

os.environ.setdefault("PGCLIENTENCODING", "UTF8")

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "gp_local")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASSWORD", "")

def connect():
    dsn = (
        f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} "
        f"user={PG_USER} password={PG_PASS} application_name=run_sql"
    )
    return psycopg2.connect(dsn=dsn, options='-c client_encoding=UTF8')

def split_sql(script: str):
    """
    Split simples por ';' em fim de comando.
    Suficiente para nossos checks (sem funções/plpgsql complexas).
    """
    parts = []
    acc = []
    for line in script.splitlines():
        acc.append(line)
        if line.strip().endswith(";"):
            parts.append("\n".join(acc))
            acc = []
    if acc:
        parts.append("\n".join(acc))
    # Remove vazios/comentários
    out = []
    for stmt in parts:
        s = stmt.strip()
        if not s:
            continue
        # ignora blocos só com comentários
        if all(x.strip().startswith("--") or not x.strip() for x in s.splitlines()):
            continue
        out.append(s)
    return out

def print_table(rows, max_rows=200):
    if not rows:
        print("(sem linhas)")
        return
    # cabeçalhos
    cols = list(rows[0].keys())
    # largura coluna básica
    widths = [max(len(c), *(len(str(r[c])) if r[c] is not None else 0 for r in rows[:max_rows])) for c in cols]
    # header
    line = " | ".join(c.ljust(w) for c, w in zip(cols, widths))
    print(line)
    print("-" * len(line))
    # linhas
    for i, r in enumerate(rows[:max_rows], 1):
        print(" | ".join((str(r[c]) if r[c] is not None else "").ljust(w) for c, w in zip(cols, widths)))
    if len(rows) > max_rows:
        print(f"... ({len(rows)-max_rows} linhas adicionais)")

def main():
    if len(sys.argv) < 2:
        print("Uso: python etl/run_sql.py <arquivo.sql>")
        sys.exit(1)

    sql_path = sys.argv[1]
    if not os.path.isfile(sql_path):
        print(f"Arquivo não encontrado: {sql_path}")
        sys.exit(2)

    with open(sql_path, "r", encoding="utf-8") as f:
        script = f.read()

    stmts = split_sql(script)
    if not stmts:
        print("Nenhuma instrução SQL encontrada.")
        sys.exit(0)

    with connect() as con, con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        con.autocommit = True
        for i, stmt in enumerate(stmts, 1):
            print("\n" + "="*80)
            print(f"[{i}/{len(stmts)}] Executando:\n{stmt}")
            try:
                cur.execute(stmt)
                # Se há resultado (SELECT), cur.description != None
                if cur.description:
                    rows = cur.fetchall()
                    print("\nResultado:")
                    print_table(rows)
                else:
                    print(f"\nOK. {cur.rowcount if cur.rowcount != -1 else 0} linha(s) afetada(s).")
            except Exception as e:
                print("\n*** ERRO ***")
                print(e)
                # continua para próximos statements

if __name__ == "__main__":
    main()
