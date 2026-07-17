from __future__ import annotations

import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Protocol, TypeVar


class DataSourceError(RuntimeError):
    """Base class for safe provider errors."""


class TransientSourceError(DataSourceError):
    """Retryable timeout, rate-limit, or temporary provider failure."""


class PermanentSourceError(DataSourceError):
    """Non-retryable request, permission, or unsupported-symbol failure."""


class SchemaValidationError(DataSourceError):
    """Provider response no longer matches the expected contract."""


class DataQualityError(DataSourceError):
    """Records are structurally valid but violate financial invariants."""


@dataclass(frozen=True, slots=True)
class DataRequest:
    dataset: str
    symbols: tuple[str, ...]
    start_date: date
    end_date: date
    incremental: bool = True

    def __post_init__(self) -> None:
        if not self.dataset.strip():
            raise ValueError("dataset must not be empty")
        if not self.symbols:
            raise ValueError("symbols must not be empty")
        if self.start_date > self.end_date:
            raise ValueError("start_date must not be after end_date")


@dataclass(frozen=True, slots=True)
class SourceRecord:
    symbol: str
    event_time: datetime
    available_at: datetime
    payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not self.symbol.strip():
            raise ValueError("symbol must not be empty")
        if self.event_time.tzinfo is None or self.available_at.tzinfo is None:
            raise ValueError("event_time and available_at must be timezone-aware")
        if self.available_at < self.event_time:
            raise ValueError("available_at must not be before event_time")


@dataclass(frozen=True, slots=True)
class BatchError:
    symbol: str | None
    error_type: str
    message: str
    retryable: bool


@dataclass(frozen=True, slots=True)
class DataBatch:
    batch_id: str
    dataset: str
    source: str
    requested_at: datetime
    fetched_at: datetime
    records: tuple[SourceRecord, ...]
    errors: tuple[BatchError, ...] = field(default_factory=tuple)

    @property
    def succeeded(self) -> bool:
        return bool(self.records) and not self.errors

    @classmethod
    def create(
        cls,
        *,
        dataset: str,
        source: str,
        records: list[SourceRecord],
        errors: list[BatchError] | None = None,
        requested_at: datetime | None = None,
    ) -> "DataBatch":
        now = datetime.now(timezone.utc)
        return cls(
            batch_id=str(uuid.uuid4()),
            dataset=dataset,
            source=source,
            requested_at=requested_at or now,
            fetched_at=now,
            records=tuple(records),
            errors=tuple(errors or []),
        )


class DataSource(Protocol):
    name: str

    def fetch(self, request: DataRequest) -> DataBatch:
        ...


T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay_seconds: float = 0.25


def with_retry(
    operation: Callable[[], T],
    *,
    policy: RetryPolicy | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    policy = policy or RetryPolicy()
    last_error: TransientSourceError | None = None
    for attempt in range(policy.max_attempts):
        try:
            return operation()
        except TransientSourceError as exc:
            last_error = exc
            if attempt + 1 < policy.max_attempts:
                sleep(policy.base_delay_seconds * (2**attempt))
    assert last_error is not None
    raise last_error
