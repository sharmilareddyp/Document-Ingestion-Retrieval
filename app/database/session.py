"""Session factory and short transaction scopes."""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import Settings
from app.core.exceptions import DatabaseFailure, ServiceUnavailableError
from app.core.logging import get_logger

logger = get_logger(__name__)
_engines: dict[str, Engine] = {}
_factories: dict[str, sessionmaker] = {}


def get_engine(settings: Settings) -> Engine:
    engine = _engines.get(settings.database_url)
    if engine is None:
        engine = create_engine(
            settings.database_url,
            pool_pre_ping=True,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            future=True,
        )
        _engines[settings.database_url] = engine
    return engine


def get_session_factory(settings: Settings) -> sessionmaker[Session]:
    factory = _factories.get(settings.database_url)
    if factory is None:
        factory = sessionmaker(
            bind=get_engine(settings), autoflush=False, expire_on_commit=False, future=True
        )
        _factories[settings.database_url] = factory
    return factory


def reset_engine_cache() -> None:
    for engine in _engines.values():
        engine.dispose()
    _engines.clear()
    _factories.clear()


@contextmanager
def session_scope(settings: Settings) -> Iterator[Session]:
    factory = get_session_factory(settings)
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def check_database(settings: Settings) -> None:
    try:
        with get_engine(settings).connect() as connection:
            connection.execute(text("SELECT 1"))
    except Exception as exc:  # pragma: no cover - message is asserted via wrapper
        raise ServiceUnavailableError("Database is unavailable") from exc


def wait_for_database(settings: Settings) -> None:
    deadline = time.monotonic() + settings.db_wait_timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            check_database(settings)
            return
        except ServiceUnavailableError as exc:
            last_error = exc
            time.sleep(1)
    logger.error(
        "database did not become ready", extra={"structured": {"operation": "wait_for_database"}}
    )
    raise DatabaseFailure("Database did not become ready before the timeout") from last_error
