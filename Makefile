PYTHON ?= python3.12

.PHONY: install test lint format migrate api worker up down seed demo benchmark

install:
	$(PYTHON) -m venv .venv
	.venv/bin/pip install -e ".[dev]"

lint:
	.venv/bin/ruff check app tests scripts airflow migrations
	.venv/bin/ruff format --check app tests scripts airflow migrations

format:
	.venv/bin/ruff check --fix app tests scripts benchmarks airflow/dags
	.venv/bin/ruff format app tests scripts benchmarks airflow/dags

test:
	.venv/bin/pytest

migrate:
	.venv/bin/alembic upgrade head

api:
	.venv/bin/uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

worker:
	.venv/bin/python -m app.workers.worker

up:
	docker compose up -d --build

down:
	docker compose down

seed:
	.venv/bin/python scripts/seed.py

demo:
	.venv/bin/python scripts/demo.py

benchmark:
	.venv/bin/python scripts/benchmark.py

sample-data:
	.venv/bin/python scripts/generate_sample_data.py

sample-api:
	.venv/bin/python scripts/sample_api.py
