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

```bash
uv run python main.py
```

## Test

```bash
uv run pytest -v
```

## Deploy

Deployed on Coolify via Nixpacks. See `nixpacks.toml`.
