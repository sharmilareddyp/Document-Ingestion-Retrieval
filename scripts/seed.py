"""Register the sample filesystem source and the records schema contract."""

from __future__ import annotations

import os
from pathlib import Path

from app.core.config import get_settings
from app.processing.schema_drift import SAMPLE_RECORDS_CONTRACT
from app.services.context import build_context
from app.services.ingestion_service import IngestionService

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    service = IngestionService(build_context(get_settings()))
    mixed = ROOT / "sample_data" / "mixed"
    try:
        source = service.register_source(
            name="sample-files",
            source_type="filesystem",
            config={"root_path": str(mixed), "detect_deletions": True},
        )
        print("registered", source["source_id"])
    except Exception as exc:
        print("filesystem source:", exc)
    api_url = os.environ.get("SAMPLE_API_URL", "http://127.0.0.1:8090/records")
    try:
        source = service.register_source(
            name="sample-api",
            source_type="rest_api",
            config={
                "url": api_url,
                "records_path": "items",
                "id_field": "id",
                "pagination": {
                    "strategy": "page",
                    "page_param": "page",
                    "page_size_param": "page_size",
                    "page_size": 2,
                },
                "text_fields": ["body"],
                "title_field": "title",
                "updated_field": "updated_at",
                "detect_deletions": False,
            },
            contract=SAMPLE_RECORDS_CONTRACT,
            contract_name="sample-records",
        )
        print("registered", source["source_id"])
    except Exception as exc:
        print("api source:", exc)


if __name__ == "__main__":
    main()
