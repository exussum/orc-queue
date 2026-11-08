from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Callable, Generic, NewType, Optional, TypeVar

from cronsim import CronSim

T = TypeVar("T")


AwareDatetime = NewType("AwareDatetime", datetime)


class OnFull(StrEnum):
    REJECT = "reject"
    EVICT = "evict"


class Event(StrEnum):
    STARTED = "started"
    SHUTDOWN = "shutdown"
    PAUSED = "paused"
    RESUMED = "resumed"
    RESET = "reset"
    ADDED = "added"
    MODIFIED = "modified"
    EVICTED = "evicted"
    CANCELED = "canceled"
    EXPIRED = "expired"
    SKIPPED = "skipped"
    SUBMITTED = "submitted"
    EXECUTED = "executed"
    ERROR = "error"


Events = dict[Event, list[Callable[..., None]]]


@dataclass
class Job(Generic[T]):
    run_at: AwareDatetime
    expires_at: Optional[AwareDatetime]
    fn: Callable[..., Any]
    payload: Optional[T] = None
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    disable: bool = False
    name: Optional[str] = None
    context: Any = field(default=None, compare=False, repr=False)


@dataclass
class CronJob(Generic[T]):
    run_at: AwareDatetime
    fn: Callable[..., Any]
    payload: Optional[T] = None
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    disable: bool = False
    name: Optional[str] = None
    context: Any = field(default=None, compare=False, repr=False)
    cron: str = field(kw_only=True)
    id_template: str = field(kw_only=True, default="{uuid}")

    def __post_init__(self) -> None:
        CronSim(self.cron, self.run_at)


AnyJob = Job[T] | CronJob[T]


def aware(dt: datetime, what: str = "datetime") -> AwareDatetime:
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError(f"{what} must be timezone-aware")
    return AwareDatetime(dt)


def utcnow() -> AwareDatetime:
    return AwareDatetime(datetime.now(timezone.utc))
