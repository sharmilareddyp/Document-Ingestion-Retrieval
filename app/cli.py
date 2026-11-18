"""Command line entry points for local operation."""

from __future__ import annotations

import argparse
import json
from uuid import UUID

from app.core.logging import configure_logging
from app.database.session import wait_for_database
from app.services.context import build_context
from app.services.indexing_service import IndexingService
from app.services.ingestion_service import IngestionService
from app.vectorstores.pinecone_store import setup_pinecone_index


def main() -> None:
    parser = argparse.ArgumentParser(description="Document ingestion operations")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("wait-db")
    sub.add_parser("setup-pinecone")
    reconcile = sub.add_parser("reconcile")
    reconcile.add_argument("--limit", type=int, default=200)
    run_job = sub.add_parser("run-job")
    run_job.add_argument("job_id")
    sub.add_parser("worker")
    args = parser.parse_args()
    configure_logging()
    if args.command == "wait-db":
        wait_for_database(build_context().settings)
        return
    if args.command == "setup-pinecone":
        print(json.dumps(setup_pinecone_index(build_context().settings)))
        return
    if args.command == "reconcile":
        print(json.dumps(IndexingService(build_context()).flush(limit=args.limit)))
        return
    if args.command == "run-job":
        print(json.dumps(IngestionService(build_context()).run_job(UUID(args.job_id)), default=str))
        return
    if args.command == "worker":
        from app.workers.worker import serve

        serve()


if __name__ == "__main__":
    main()
