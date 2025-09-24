# Gestão de Produção (Microsys → ETL → API → Front)

> **Objetivo**  
> Copiar dados do Microsys (Firebird), organizar no Postgres (schema simplificado),
> expor via **FastAPI** e visualizar no **Front** (HTML/JS) com painéis, filtros e fila da **Pintura**.

---

## 📁 Estrutura do projeto

gestao-prod/
├─ backend/
│ ├─ app.py # FastAPI (somente leitura)
│ └─ ... # (outros utilitários)
├─ etl/
│ ├─ 00_init_pg.py # cria DB e aplica schema
│ ├─ 04_copiar_janela.py # copia janela de OPs do Microsys → Postgres
│ ├─ run_sql.py # utilitário para rodar SQLs
│ ├─ sql/
│ │ ├─ pg_schema.sql # schema simplificado (op, op_item, roteiro, andamento_setor, cfg_pintura_prod)
│ │ └─ quick_check.sql # verificações rápidas após ETL
│ └─ requirements.txt
├─ frontend/
│ ├─ index.html # Painel principal (OPs, agregados, série, etc.)
│ ├─ pintura.html # Painel do Operador de Pintura (fila)
│ └─ assets/ # (opcional) imagens para prints/ícones
├─ docs/
│ └─ img/
│ ├─ painel.png # ⬅ coloque seus prints aqui
│ ├─ agregados.png
│ └─ pintura_fila.png
├─ .gitignore
└─ README.md


> **Dica:** ajuste os caminhos das imagens nos trechos de “Prints” abaixo para apontar para `docs/img/*`.

---

## 🧪 Pré-requisitos

- **Python 3.10+**
- **PostgreSQL** 14+ (localhost OK)
- (Se vai ler Microsys) **Firebird client** + Python lib `firebirdsql`
- VS Code recomendado (Live Server para abrir o front)

---

## ⚙️ Configuração de ambiente

Crie e ative a venv (Windows PowerShell):

```powershell
python -m venv .venv
. .\.venv\Scripts\Activate.ps1

Instale as dependências:

pip install -r .\etl\requirements.txt
pip install fastapi uvicorn psycopg2-binary python-dotenv

Crie os arquivos .env:

backend/.env (ou etl/.env, o backend tenta carregar de ambos)

# Postgres
PG_HOST=localhost
PG_PORT=5432
PG_DB=gp_local
PG_USER=postgres
PG_PASSWORD=postgres

# (Opcional) Microsys / Firebird - usados pelo ETL
FB_HOST=localhost
FB_PORT=3050
FB_DB=C:\Microsys\BASE.FDB
FB_USER=sysdba
FB_PASSWORD=masterkey

O backend já configura CORS liberado para dev.

🗄️ Passo 1 — Criar banco + schema

python .\etl\00_init_pg.py

Saída esperada (resumo):

Database gp_local já existe (ou criado).
Aplicando schema: etl\sql\pg_schema.sql
OK.
O schema cria as tabelas op, op_item, roteiro, andamento_setor, cfg_pintura_prod
e carrega os códigos de pintura em cfg_pintura_prod.

📥 Passo 2 — Copiar dados (janela de OPs)

Exemplo (Filial 1, por validade, de 01/05 a 30/10):
python .\etl\04_copiar_janela.py --filial 1 --date-field validade --from 2025-05-01 --to 2025-10-30

Dry-run (sem gravar):
python .\etl\04_copiar_janela.py --filial 1 --date-field validade --from 2025-05-01 --to 2025-10-30 --dry-run

Verificação rápida:
python .\etl\run_sql.py .\etl\sql\quick_check.sql

Saídas úteis:

Quantidade de OPs cabeçalho (op)

Linhas em op_item

Códigos em cfg_pintura_prod

m² de pintura de uma OP (exemplo no SQL)

🚀 Passo 3 — Subir a API
uvicorn backend.app:app --host 127.0.0.1 --port 8000 --reload

Teste rápido:
Invoke-RestMethod http://127.0.0.1:8000/health

Endpoints principais

GET /health → status

GET /ops → listagem com filtros/paginação/ordenação
params: filial, date_field (validade|prev_inicio|emissao), from, to, days_back, days_ahead,
status, q, cor_contains, percent_min, percent_max, page, page_size, order_by, order_dir

GET /ops/{op_id} → detalhe + itens + roteiro + pintura_m2 (total, produzida, saldo)

GET /ops/faltando-pintura → OPs em que só falta Pintura (usa cfg_pintura_prod + heurística)

GET /dashboard → agregados (by_status, by_color, series, avg_percent)

GET /pintura/fila → (novo) fila da Pintura (para o frontend/pintura.html)

Dica: para testar rapidamente:
Invoke-RestMethod "http://127.0.0.1:8000/ops?filial=1&date_field=validade&from=2025-05-01&to=2025-10-30"
Invoke-RestMethod "http://127.0.0.1:8000/ops/faltando-pintura?filial=1&date_field=validade&from=2025-05-01&to=2025-10-30"
Invoke-RestMethod "http://127.0.0.1:8000/ops/6372"

🖥️ Passo 4 — Subir o Front
Opção A — VS Code (Live Server)

Clique com botão direito em frontend/index.html → Open with Live Server.

Normalmente abre em http://127.0.0.1:5500/frontend/index.html.

Opção B — Servidor simples do Python
cd frontend
python -m http.server 5500
# abra http://127.0.0.1:5500/frontend/index.html

Importante: a API precisa estar no http://127.0.0.1:8000 (CORS já liberado).

🧩 Prints (coloque suas imagens)

Salve as imagens em docs/img/ e ajuste as URLs abaixo se necessário.

Painel (index)

Agregados por Cor e Status

Fila de Pintura (operador)

🧠 Dicas de uso

Filtro rápido de status (no front):

Todas

Abertas

Entrada Parcial

Faltando apenas Pintura (usa /ops/faltando-pintura)

Contorno/alerta por validade:

≤ 5 dias: amarelo

≤ 3 dias: vermelho

m² de Pintura:

Em /ops/{op_id} e (quando aplicável) na fila de Pintura, calculado a partir de itens cujo pro_codigo aparece em cfg_pintura_prod.

🛠️ Solução de problemas

CORS bloqueado no navegador

Confirme API em http://127.0.0.1:8000 e Front em http://127.0.0.1:5500

O app.py já adiciona CORSMiddleware(allow_origins=["*"])

Tabela cfg_pintura_prod inexistente

Rode 00_init_pg.py (ele aplica pg_schema.sql com a seed)

Sem OPs no período

Execute o ETL novamente com a janela desejada (04_copiar_janela.py)

Erro de conexão Postgres

Verifique credenciais no .env, serviço ativo e porta 5432

🧭 Roadmap rápido

 ETL Firebird → Postgres

 API FastAPI com endpoints de OPs, Dashboard e Pintura

 Front (painel + fila de Pintura)

 Painéis por setor (Perfiladeira/Serralheria/Eixo) — próximo

 Painel Calendário (mensal)

 Módulos de Entrega/Instalação

🧾 Licença

Uso interno. Ajuste conforme a necessidade da empresa.


---

### `CONTRIBUTING.md`

```markdown
# Contribuindo

Obrigado por contribuir! Aqui vai o **fluxo mínimo** para manter o projeto organizado.

## Branches

- `main` → estável (deploy / produção)
- `dev`  → integração de features (opcional)
- `feature/nome-curto` → trabalho do dia a dia

Crie sua branch de feature a partir de `main` (ou `dev`, se estiver usando):

```bash
git checkout main
git pull
git checkout -b feature/painel-pintura

Commits (Conventional Commits)

Use a convenção para facilitar changelog:

feat: nova funcionalidade

fix: correção de bug

docs: documentação (README, etc.)

chore: tarefas (infra, setup, etc.)

refactor: refatoração sem alterar comportamento

style: formatação sem lógica

test: testes

Exemplos:
feat(pintura): endpoint /pintura/fila com agregados por cor
fix(front): corrige formatação dd/mm/aaaa no cartão de OP
docs: adiciona README com passo-a-passo ETL→API→Front

subir git: 

git add .
git commit -m "chore: local snapshot before syncing with remote"

git push -u origin main
