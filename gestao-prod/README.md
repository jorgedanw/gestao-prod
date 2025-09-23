# Gestão de Produção — Painel + API

Monorepo simples com:
- **backend/**: API FastAPI (somente leitura)
- **etl/**: scripts para copiar dados do Microsys para Postgres
- **frontend/**: HTML/JS estático do painel

## Requisitos
- Python 3.10+
- Postgres 13+
- Git

## Como rodar (dev)

### 1) Virtualenv e dependências
```powershell
python -m venv .venv
. .venv/Scripts/Activate.ps1
pip install -r etl/requirements.txt
