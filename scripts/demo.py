"""Run the documented local scenarios against PostgreSQL and the fake providers."""

from __future__ import annotations

import os
import textwrap
from pathlib import Path

from app.processing.schema_drift import SAMPLE_RECORDS_CONTRACT
from app.services.context import build_context
from app.services.document_service import DocumentService
from app.services.indexing_service import IndexingService
from app.services.ingestion_service import IngestionService
from app.services.quality_service import QualityService


def require_database() -> None:
    if not os.environ.get("DATABASE_URL"):
        raise SystemExit("Set DATABASE_URL before running the demo.")


def main() -> None:
    require_database()
    root = Path(__file__).resolve().parents[1]
    os.environ.setdefault("EMBEDDING_BACKEND", "fake")
    os.environ.setdefault("VECTOR_BACKEND", "memory")
    from app.core.config import reset_settings_cache
    from app.database.session import reset_engine_cache
    from app.services.context import reset_context_cache

    reset_settings_cache()
    reset_engine_cache()
    reset_context_cache()
    context = build_context()
    ingestion = IngestionService(context)
    documents = DocumentService(context)
    quality = QualityService(context)
    mixed = root / "sample_data" / "mixed"
    if not mixed.exists():
        raise SystemExit("Run python scripts/generate_sample_data.py first.")
    source = ingestion.register_source(
        name="demo-mixed",
        source_type="filesystem",
        config={"root_path": str(mixed), "detect_deletions": False},
    )
    job = ingestion.enqueue_source(source["source_id"], trigger="manual", start_immediately=True)
    finished = ingestion.run_job(job["job_id"])
    print(
        textwrap.dedent(
            f"""
        Scenario 1 mixed ingestion
          job={finished["job_id"]} status={finished["status"]}
          processed={finished["processed_documents"]} skipped={finished["skipped_documents"]}
          failed={finished["failed_documents"]} quarantined={finished["quarantined_documents"]}
        """
        )
    )
    again = ingestion.run_job(
        ingestion.enqueue_source(source["source_id"], trigger="manual", start_immediately=True)[
            "job_id"
        ]
    )
    print(
        f"Scenario 4 unchanged re-ingest skipped={again['skipped_documents']} processed={again['processed_documents']}"
    )
    target = mixed / "onboarding.txt"
    original = target.read_text(encoding="utf-8")
    target.write_text(
        original + "\nManagers approve production access separately.\n", encoding="utf-8"
    )
    changed = ingestion.run_job(
        ingestion.enqueue_source(source["source_id"], trigger="manual", start_immediately=True)[
            "job_id"
        ]
    )
    print(f"Scenario 4 changed re-ingest processed={changed['processed_documents']}")
    target.write_text(original, encoding="utf-8")
    listed, total = documents.list_documents(source_id=source["source_id"], limit=20)
    print(f"documents={total}")
    if listed:
        versions = documents.list_versions(listed[0]["document_id"])
        print(f"first document versions={len(versions)} title={listed[0]['title']}")
    audit = quality.audit()
    print(f"Scenario 8 quality audit checked={audit['checked']} failed={audit['failed']}")
    store = context.vector_store
    real_upsert = store.upsert

    def fail_once(vectors):
        store.upsert = real_upsert
        raise RuntimeError("simulated vector outage")

    store.upsert = fail_once
    failure_source = ingestion.register_source(
        name="demo-failure",
        source_type="filesystem",
        config={"root_path": str(mixed), "detect_deletions": False},
    )
    failed = ingestion.run_job(
        ingestion.enqueue_source(
            failure_source["source_id"], trigger="manual", force=True, start_immediately=True
        )["job_id"]
    )
    print(
        f"Scenario 6 forced failure status={failed['status']} failed_documents={failed['failed_documents']}"
    )
    recovered = IndexingService(context).flush(limit=500)
    print(f"Scenario 6 reconciliation {recovered}")
    print("Schema contract available for the sample API:", "fields" in SAMPLE_RECORDS_CONTRACT)


if __name__ == "__main__":
    main()
