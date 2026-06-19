# Distance-Updater

Bulk-updates the `distancia` column in `solicitacao` for completed requests that still have distance = 0.

Distances are resolved from a local CSV cache (`distancias.csv`) first, falling back to the Google Maps Distance Matrix API. New API results are appended to the CSV for future lookups. The final stored value is `round(raw_km + 40)`.

**Version:** ßeta

## Setup

```bash
uv sync            # install dependencies
cp .env.example .env   # fill in your credentials
```

## Run

### `main.py` — rotina padrão (deploy / uso diário)

Atualiza solicitações **concluídas** (`data_conclusao` preenchida) com `distancia = 0`.

**Casos de uso:**

| Situação | Comando |
|----------|---------|
| Rodar o job agendado no Coolify (fluxo normal) | `uv run python main.py` |
| Processar o backlog atual de solicitações concluídas sem distância | `uv run python main.py` |
| Após novas conclusões no sistema, preencher distâncias faltantes | `uv run python main.py` |

**O que faz em cada execução:**

1. Busca no Postgres solicitações concluídas com `distancia = 0`
2. Resolve a distância pelo cache `distancias.csv`
3. Se a rota não existir no cache, consulta a Google Distance Matrix API e grava no CSV
4. Salva no banco `round(distancia_km + 40)` como inteiro

**Não possui parâmetros de linha de comando.**

```bash
uv run python main.py
```

---

### `run_legacy.py` — carga histórica / backfill

Mesma lógica de distância do `main.py`, mas voltado para **todo o histórico até hoje** de solicitações concluídas sem distância registrada. Aceita filtro por data e modo simulação.

**Casos de uso:**

| Situação | Comando |
|----------|---------|
| Ver quantos registros históricos faltam atualizar (sem alterar nada) | `uv run python run_legacy.py --dry-run` |
| Ver quantos registros faltam a partir de um mês/ano | `uv run python run_legacy.py --dry-run -m 5 -YYYY 2025` |
| Backfill completo de todo o histórico pendente | `uv run python run_legacy.py` |
| Backfill apenas de solicitações concluídas a partir de maio/2025 | `uv run python run_legacy.py -m 5 -YYYY 2025` |
| Validar volume antes de rodar carga pesada no banco/Google | `uv run python run_legacy.py --dry-run` → depois rodar sem `--dry-run` |

**Parâmetros:**

| Parâmetro | Descrição |
|-----------|-----------|
| `--dry-run` | Apenas conta quantos registros seriam atualizados. Não consulta Google, não altera `distancias.csv` e não grava no banco. |
| `-m MONTH` | Mês inicial do filtro (1–12). Deve ser usado junto com `-YYYY`. |
| `-YYYY YEAR` | Ano inicial do filtro. Filtra por `data_conclusao >= primeiro dia do mês/ano`. |

**Exemplos:**

```bash
# simulação — todo o histórico
uv run python run_legacy.py --dry-run

# simulação — a partir de maio/2025
uv run python run_legacy.py --dry-run -m 5 -YYYY 2025

# execução real — todo o histórico
uv run python run_legacy.py

# execução real — a partir de maio/2025
uv run python run_legacy.py -m 5 -YYYY 2025
```

**Quando usar `main.py` vs `run_legacy.py`:**

| | `main.py` | `run_legacy.py` |
|---|-----------|-----------------|
| Objetivo | rotina operacional / deploy | backfill histórico |
| Escopo temporal | pendências atuais (sem filtro de data) | histórico até hoje, com filtro opcional |
| `--dry-run` | não | sim |
| Filtro `-m` / `-YYYY` | não | sim |

## Test

```bash
uv run pytest -v
```

## Deploy

Deployed on Coolify via Nixpacks. See `nixpacks.toml`.
