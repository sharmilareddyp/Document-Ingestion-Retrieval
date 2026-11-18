"""Batch isolation.

One bad document is recorded and the rest of the batch continues. Database
failures abort the batch because later writes would fail the same way.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from typing import TypeVar

from app.core.exceptions import DatabaseFailure
from app.processing.state_machine import is_retryable_exception

T = TypeVar("T")
R = TypeVar("R")


@dataclass
class ItemFailure:
    item: object
    error: BaseException
    retryable: bool
    category: str


def process_each(
    items: Iterable[T],
    handler: Callable[[T], R],
    *,
    abort_on: tuple[type[BaseException], ...] = (DatabaseFailure,),
) -> Iterator[R | ItemFailure]:
    for item in items:
        try:
            yield handler(item)
        except abort_on:
            raise
        except Exception as exc:
            category = getattr(exc, "category", None) or type(exc).__name__
            yield ItemFailure(
                item=item, error=exc, retryable=is_retryable_exception(exc), category=str(category)
            )
