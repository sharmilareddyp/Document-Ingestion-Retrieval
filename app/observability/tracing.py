"""Optional OpenTelemetry wrapper.

The application depends only on the OpenTelemetry API. Without a configured SDK the
tracer is a no-op, so local tests and Airflow tasks do not need a collector.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from app.core.logging import get_logger

logger = get_logger(__name__)
_CONFIGURED = False


def configure_tracing(endpoint: str) -> None:
    global _CONFIGURED
    if _CONFIGURED or not endpoint:
        return
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        provider = TracerProvider(
            resource=Resource.create({"service.name": "document-ingestion-retrieval"})
        )
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
        trace.set_tracer_provider(provider)
        _CONFIGURED = True
    except Exception:
        logger.warning(
            "OpenTelemetry SDK is not installed; tracing stays disabled",
            extra={"structured": {"operation": "configure_tracing", "error_category": "telemetry"}},
        )


@contextmanager
def span(name: str) -> Iterator[None]:
    try:
        from opentelemetry import trace

        tracer = trace.get_tracer("document-ingestion-retrieval")
        with tracer.start_as_current_span(name):
            yield
    except Exception:
        yield
