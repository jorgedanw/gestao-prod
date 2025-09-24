# etl/00_init_pg.py
# Cria o banco (se não existir) e aplica o schema pg_schema.sql
import os, psycopg2, psycopg2.extras
from psycopg2 import sql
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")
SCHEMA_PATH = os.path.join(BASE_DIR, "sql", "pg_schema.sql")
load_dotenv(ENV_PATH)

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "gp_local")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASSWORD", "")

def connect(dbname):
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=dbname, user=PG_USER, password=PG_PASS)

def db_exists():
    try:
        con = connect(PG_DB); con.close()
        return True
    except Exception:
        return False

def create_db():
    con = connect("postgres"); con.autocommit = True
    cur = con.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (PG_DB,))
    if not cur.fetchone():
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(PG_DB)))
        print(f"Database {PG_DB} criado.")
    else:
        print(f"Database {PG_DB} já existe (via postgres).")
    cur.close(); con.close()

def apply_schema():
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        ddl = f.read()
    con = connect(PG_DB); con.autocommit = False
    cur = con.cursor()
    try:
        cur.execute(ddl)
        con.commit()
        print(f"Schema aplicado com sucesso a {PG_DB}.")
    except Exception as e:
        con.rollback()
        raise
    finally:
        cur.close(); con.close()

if __name__ == "__main__":
    if db_exists():
        print(f"Database {PG_DB} já existe.")
    else:
        create_db()
    print(f"Aplicando schema: {SCHEMA_PATH}")
    apply_schema()
