from __future__ import annotations

import bisect
import logging
import pickle
import sqlite3
from dataclasses import fields
from datetime import datetime, timedelta, timezone
from typing import Any, Generic, NamedTuple, Optional, Protocol, TypeVar

from . import model as m

T = TypeVar("T")


_log = logging.getLogger("orc_queue")


class UnloadableJob(RuntimeError):
    def __init__(self, job_id: str, cause: BaseException):
        super().__init__(f"job {job_id!r} could not be loaded and was deleted: {cause!r}")
        self.job_id = job_id
        self.__cause__ = cause


class Store(Protocol[T]):
    def get(self, job_id: str) -> Optional[m.AnyJob[T]]: ...
    def put(self, job: m.AnyJob[T]) -> None: ...
    def delete(self, job_id: str) -> None: ...
    def first(self) -> Optional[m.AnyJob[T]]: ...
    def droppable(self, now: m.AwareDatetime) -> list[m.AnyJob[T]]: ...
    def all(self) -> list[m.AnyJob[T]]: ...
    def count(self) -> int: ...


class SortKey(NamedTuple):
    disable: bool
    run_at: datetime
    id: str


class MemoryStore(Generic[T]):
    def __init__(self) -> None:
        self._jobs: dict[str, m.AnyJob[T]] = {}
        self._keys: dict[str, SortKey] = {}
        self._order: list[SortKey] = []

    def get(self, job_id: str) -> Optional[m.AnyJob[T]]:
        return self._jobs.get(job_id)

    def put(self, job: m.AnyJob[T]) -> None:
        if job.id in self._keys:
            self._drop_key(job.id)
        key = _sort_key(job)
        self._jobs[job.id] = job
        self._keys[job.id] = key
        bisect.insort(self._order, key)

    def delete(self, job_id: str) -> None:
        del self._jobs[job_id]
        self._drop_key(job_id)

    def first(self) -> Optional[m.AnyJob[T]]:
        if not self._order or self._order[0].disable:
            return None
        return self._jobs[self._order[0].id]

    def droppable(self, now: m.AwareDatetime) -> list[m.AnyJob[T]]:
        return [job for job in self.all() if _drop(job, now)]

    def all(self) -> list[m.AnyJob[T]]:
        return [self._jobs[key.id] for key in self._order]

    def count(self) -> int:
        return len(self._jobs)

    def _drop_key(self, job_id: str) -> None:
        key = self._keys.pop(job_id)
        del self._order[bisect.bisect_left(self._order, key)]


class SqliteStore(Generic[T]):
    def __init__(self, path: str, name: str = "default", discard_after: timedelta = timedelta(minutes=5)):
        self._path = path
        self._name = name
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.executescript(_SCHEMA)
        self._purge_unloadable()
        self._discard_late(discard_after)

    def close(self) -> None:
        self._db.close()

    def get(self, job_id: str) -> Optional[m.AnyJob[T]]:
        row = self._db.execute(_GET, (self._name, job_id)).fetchone()
        return self._load(row) if row else None

    def put(self, job: m.AnyJob[T]) -> None:
        blob = pickle.dumps(job)  # nosemgrep: python.lang.security.deserialization.pickle.avoid-pickle
        self._db.execute(
            _PUT,
            (
                self._name,
                job.id,
                int(job.disable),
                int(isinstance(job, m.CronJob)),
                _stamp(job.expires_at) if isinstance(job, m.Job) else None,
                _sort_key(job).run_at.isoformat(),
                blob,
            ),
        )
        self._db.commit()

    def delete(self, job_id: str) -> None:
        cur = self._db.execute(_DELETE, (self._name, job_id))
        self._db.commit()
        if cur.rowcount == 0:
            raise KeyError(job_id)

    def first(self) -> Optional[m.AnyJob[T]]:
        row = self._db.execute(_FIRST, (self._name,)).fetchone()
        return self._load(row) if row else None

    def droppable(self, now: m.AwareDatetime) -> list[m.AnyJob[T]]:
        stamp = now.astimezone(timezone.utc).isoformat()
        rows = self._db.execute(_DROPPABLE, (self._name, stamp, stamp)).fetchall()
        return [self._load(row) for row in rows]

    def all(self) -> list[m.AnyJob[T]]:
        rows = self._db.execute(_ALL, (self._name,)).fetchall()
        return [self._load(row) for row in rows]

    def count(self) -> int:
        return int(self._db.execute(_COUNT, (self._name,)).fetchone()[0])

    def _purge_unloadable(self) -> None:
        dropped = []
        for job_id, blob in self._db.execute(_ROWS, (self._name,)).fetchall():
            try:
                _decode(job_id, blob)
            except Exception as exc:
                _log.debug("%s/%s: startup dropped job %s: %r", self._path, self._name, job_id, exc)
                dropped.append(job_id)
        if dropped:
            self._db.executemany(_DELETE, [(self._name, j) for j in dropped])
            self._db.commit()
            _log.info("%s/%s: dropped %d unloadable job(s) at startup", self._path, self._name, len(dropped))

    def _discard_late(self, discard_after: timedelta) -> None:
        cutoff = (m.utcnow() - discard_after).astimezone(timezone.utc).isoformat()
        cur = self._db.execute(_DISCARD_LATE, (self._name, cutoff))
        self._db.commit()
        if cur.rowcount:
            _log.info("%s/%s: discarded %d job(s) more than %s late at startup", self._path, self._name, cur.rowcount, discard_after)

    def _load(self, row: Any) -> m.AnyJob[T]:
        job_id, blob = row
        try:
            return _decode(job_id, blob)
        except Exception as exc:
            _log.exception("%s/%s: unloadable job %s, deleting", self._path, self._name, job_id)
            self._db.execute(_DELETE, (self._name, job_id))
            self._db.commit()
            raise UnloadableJob(job_id, exc) from exc


def _sort_key(job: m.AnyJob[Any]) -> SortKey:
    return SortKey(job.disable, job.run_at.astimezone(timezone.utc), job.id)


def _stamp(when: Optional[datetime]) -> Optional[str]:
    return when.astimezone(timezone.utc).isoformat() if when else None


def _drop(job: m.AnyJob[Any], now: m.AwareDatetime) -> bool:
    if isinstance(job, m.Job) and job.expires_at and job.expires_at <= now:
        return True
    return job.disable and job.run_at <= now


def _decode(job_id: str, blob: bytes) -> Any:
    job = pickle.loads(blob)  # nosemgrep: python.lang.security.deserialization.pickle.avoid-pickle
    missing = [f.name for f in fields(job) if not hasattr(job, f.name)]
    if missing:
        raise ValueError(f"job {job_id!r} is missing fields {missing}")
    return job


_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    queue TEXT NOT NULL,
    id TEXT NOT NULL,
    disable INTEGER NOT NULL,
    cron INTEGER NOT NULL DEFAULT 0,
    expires_at TEXT,
    run_at TEXT NOT NULL,
    job BLOB NOT NULL,
    PRIMARY KEY (queue, id)
);
CREATE INDEX IF NOT EXISTS jobs_order ON jobs (queue, disable, run_at, id);
"""
_GET = "SELECT id, job FROM jobs WHERE queue = ? AND id = ?"
_PUT = "INSERT OR REPLACE INTO jobs (queue, id, disable, cron, expires_at, run_at, job) VALUES (?, ?, ?, ?, ?, ?, ?)"
_DELETE = "DELETE FROM jobs WHERE queue = ? AND id = ?"
_FIRST = "SELECT id, job FROM jobs WHERE queue = ? AND disable = 0 ORDER BY run_at, id LIMIT 1"
_DROPPABLE = "SELECT id, job FROM jobs WHERE queue = ? AND ((disable = 1 AND run_at <= ?) OR expires_at <= ?) ORDER BY run_at, id"
_ALL = "SELECT id, job FROM jobs WHERE queue = ? ORDER BY disable, run_at, id"
_COUNT = "SELECT COUNT(*) FROM jobs WHERE queue = ?"
_ROWS = "SELECT id, job FROM jobs WHERE queue = ?"
_DISCARD_LATE = "DELETE FROM jobs WHERE queue = ? AND cron = 0 AND run_at < ?"
