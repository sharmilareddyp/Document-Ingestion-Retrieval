#!/bin/sh
set -eu

python -m app.cli wait-db

if [ "${1:-api}" = "api" ]; then
  alembic upgrade head
  exec uvicorn app.main:app --host 0.0.0.0 --port 8000
fi

if [ "$1" = "worker" ]; then
  exec python -m app.workers.worker
fi

exec "$@"
