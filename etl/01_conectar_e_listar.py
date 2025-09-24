"""
PARTE 1 — Conectar ao MSYSDADOS.FDB e listar tabelas de usuário.

Regras:
- Leitura SOMENTE. Não modifica nada no banco do Microsys.
- Se algo falhar (ex.: fbclient ausente), a mensagem de erro vai indicar o que ajustar.

Como rodar:
    1) Ative o venv
    2) python etl/01_conectar_e_listar.py
"""

import os
from dotenv import load_dotenv
from firebird.driver import connect, get_client_version

# Carrega variáveis de ambiente (.env)
load_dotenv(dotenv_path=os.path.join("etl", ".env"))

FB_HOST = os.getenv("FIREBIRD_HOST", "localhost")
FB_PORT = os.getenv("FIREBIRD_PORT", "3050")
FB_DB   = os.getenv("FIREBIRD_DB_PATH")  # caminho completo ou alias
FB_USER = os.getenv("FIREBIRD_USER", "SYSDBA")
FB_PASS = os.getenv("FIREBIRD_PASSWORD", "masterkey")
FB_CHAR = os.getenv("FIREBIRD_CHARSET", "WIN1252")

DSN = f"{FB_HOST}/{FB_PORT}:{FB_DB}"

def listar_tabelas_usuario(con):
    """
    Retorna apenas TABELAS DE USUÁRIO (ignora views e objetos de sistema).
    """
    SQL = """
    SELECT TRIM(r.rdb$relation_name)
    FROM rdb$relations r
    WHERE r.rdb$system_flag = 0
      AND r.rdb$view_blr IS NULL
    ORDER BY 1
    """
    with con.cursor() as cur:
        cur.execute(SQL)
        return [row[0] for row in cur.fetchall()]

def amostra_tabela(con, tabela, limit=3):
    """
    Tenta trazer algumas linhas para confirmar leitura.
    """
    SQL = f"SELECT FIRST {limit} * FROM {tabela}"
    with con.cursor() as cur:
        cur.execute(SQL)
        cols = [c.column_name for c in cur.description]
        rows = cur.fetchall()
        return cols, rows

if __name__ == "__main__":
    print("== PARTE 1: Conexão e listagem ==")
    try:
        print("Versão do cliente Firebird:", get_client_version())
    except Exception as e:
        print("Aviso: não consegui ler a versão do cliente Firebird.")
        print("Dica: verifique a instalação do Firebird Client. Erro:", e)

    print("Conectando ao Firebird em:", DSN)
    con = connect(dsn=DSN, user=FB_USER, password=FB_PASS, charset=FB_CHAR)
    print("OK! Conexão estabelecida (somente leitura).")

    tabs = listar_tabelas_usuario(con)
    print(f"Total de tabelas de usuário: {len(tabs)}")
    for t in tabs[:20]:
        print(" -", t)

    # (Opcional) tente mostrar amostra de uma tabela conhecida, se existir
    candidatos = ["ORDEM_PRODUCAO", "PCP_ORP_ROTEIRO", "PEDIDOS_VENDA", "CLIENTES"]
    for nome in candidatos:
        if nome in tabs:
            print(f"\nAmostra de {nome}:")
            try:
                cols, rows = amostra_tabela(con, nome, limit=2)
                print("Colunas:", cols)
                for r in rows:
                    print("Linha:", r)
            except Exception as e:
                print("Não foi possível ler amostra de", nome, "->", e)
            break

    con.close()
    print("\nConcluído com sucesso. Nada foi modificado no MSYSDADOS.FDB.")
