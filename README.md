# GKR Trading — V1 Platform

Python monolith: research/backtest and live/paper share portfolio, risk, events, and replay.

## Setup

```bash
cd "/Users/rohandhulipalla/GKR INDUSTRIES"
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

## CLI

```bash
gkr --help
gkr init-db --db-path ./data/gkr.db
gkr ingest-instruments --db-path ./data/gkr.db
gkr ingest-bars --db-path ./data/gkr.db
gkr backtest --db-path ./data/gkr.db --session-id <uuid>
gkr paper --db-path ./data/gkr.db
gkr paper --db-path ./data/gkr.db --quiet
gkr paper --db-path ./data/gkr.db --json
gkr paper-dry-run --db-path ./data/gkr.db
gkr session-inspect --db-path ./data/gkr.db --session-id <uuid>
gkr replay --db-path ./data/gkr.db --session-id <uuid>
gkr portfolio-show --db-path ./data/gkr.db --session-id <uuid>
```

## Tests

```bash
pytest tests/ -q
```
