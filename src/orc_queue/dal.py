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
    def get(self, job_id: str) -> Optional[m.Job[T]]: ...
    def put(self, job: m.Job[T]) -> None: ...
    def delete(self, job_id: str) -> None: ...
    def first(self) -> Optional[m.Job[T]]: ...
    def skipped(self, now: m.AwareDatetime) -> list[m.Job[T]]: ...
    def all(self) -> list[m.Job[T]]: ...
    def count(self) -> int: ...


class SortKey(NamedTuple):
    disable: bool
    run_at: datetime
    id: str


class MemoryStore(Generic[T]):
    def __init__(self) -> None:
        self._jobs: dict[str, m.Job[T]] = {}
        self._keys: dict[str, SortKey] = {}
        self._order: list[SortKey] = []

    def get(self, job_id: str) -> Optional[m.Job[T]]:
        return self._jobs.get(job_id)

    def put(self, job: m.Job[T]) -> None:
        if job.id in self._keys:
            self._drop_key(job.id)
        key = _sort_key(job)
        self._jobs[job.id] = job
        self._keys[job.id] = key
        bisect.insort(self._order, key)

    def delete(self, job_id: str) -> None:
        del self._jobs[job_id]
        self._drop_key(job_id)

    def first(self) -> Optional[m.Job[T]]:
        if not self._order or self._order[0].disable:
            return None
        return self._jobs[self._order[0].id]

    def skipped(self, now: m.AwareDatetime) -> list[m.Job[T]]:
        return [self._jobs[key.id] for key in self._order if key.disable and key.run_at <= now]

    def all(self) -> list[m.Job[T]]:
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
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS jobs ("
            " queue TEXT NOT NULL, id TEXT NOT NULL, disable INTEGER NOT NULL, cron INTEGER NOT NULL DEFAULT 0,"
            " run_at TEXT NOT NULL, job BLOB NOT NULL, PRIMARY KEY (queue, id))"
        )
        if "cron" not in {row[1] for row in self._db.execute("PRAGMA table_info(jobs)")}:
            self._db.execute("ALTER TABLE jobs ADD COLUMN cron INTEGER NOT NULL DEFAULT 0")
        self._db.execute("CREATE INDEX IF NOT EXISTS jobs_order ON jobs (queue, disable, run_at, id)")
        self._db.commit()
        self._purge_unloadable()
        self._discard_late(discard_after)

    def close(self) -> None:
        self._db.close()

    def get(self, job_id: str) -> Optional[m.Job[T]]:
        row = self._db.execute("SELECT id, job FROM jobs WHERE queue = ? AND id = ?", (self._name, job_id)).fetchone()
        return self._load(row) if row else None

    def put(self, job: m.Job[T]) -> None:
        blob = pickle.dumps(job)  # nosemgrep: python.lang.security.deserialization.pickle.avoid-pickle
        self._db.execute(
            "INSERT OR REPLACE INTO jobs (queue, id, disable, cron, run_at, job) VALUES (?, ?, ?, ?, ?, ?)",
            (self._name, job.id, int(job.disable), int(isinstance(job, m.CronJob)), _sort_key(job).run_at.isoformat(), blob),
        )
        self._db.commit()

    def delete(self, job_id: str) -> None:
        cur = self._db.execute("DELETE FROM jobs WHERE queue = ? AND id = ?", (self._name, job_id))
        self._db.commit()
        if cur.rowcount == 0:
            raise KeyError(job_id)

    def first(self) -> Optional[m.Job[T]]:
        row = self._db.execute(
            "SELECT id, job FROM jobs WHERE queue = ? AND disable = 0 ORDER BY run_at, id LIMIT 1", (self._name,)
        ).fetchone()
        return self._load(row) if row else None

    def skipped(self, now: m.AwareDatetime) -> list[m.Job[T]]:
        rows = self._db.execute(
            "SELECT id, job FROM jobs WHERE queue = ? AND disable = 1 AND run_at <= ? ORDER BY run_at, id",
            (self._name, now.astimezone(timezone.utc).isoformat()),
        ).fetchall()
        return [self._load(row) for row in rows]

    def all(self) -> list[m.Job[T]]:
        rows = self._db.execute("SELECT id, job FROM jobs WHERE queue = ? ORDER BY disable, run_at, id", (self._name,)).fetchall()
        return [self._load(row) for row in rows]

    def count(self) -> int:
        return int(self._db.execute("SELECT COUNT(*) FROM jobs WHERE queue = ?", (self._name,)).fetchone()[0])

    def _purge_unloadable(self) -> None:
        dropped = []
        for job_id, blob in self._db.execute("SELECT id, job FROM jobs WHERE queue = ?", (self._name,)).fetchall():
            try:
                _decode(job_id, blob)
            except Exception as exc:
                _log.debug("%s/%s: startup dropped job %s: %r", self._path, self._name, job_id, exc)
                dropped.append(job_id)
        if dropped:
            self._db.executemany("DELETE FROM jobs WHERE queue = ? AND id = ?", [(self._name, j) for j in dropped])
            self._db.commit()
            _log.info("%s/%s: dropped %d unloadable job(s) at startup", self._path, self._name, len(dropped))

    def _discard_late(self, discard_after: timedelta) -> None:
        cutoff = (m.utcnow() - discard_after).astimezone(timezone.utc).isoformat()
        cur = self._db.execute("DELETE FROM jobs WHERE queue = ? AND cron = 0 AND run_at < ?", (self._name, cutoff))
        self._db.commit()
        if cur.rowcount:
            _log.info("%s/%s: discarded %d job(s) more than %s late at startup", self._path, self._name, cur.rowcount, discard_after)

    def _load(self, row: Any) -> m.Job[T]:
        job_id, blob = row
        try:
            return _decode(job_id, blob)
        except Exception as exc:
            _log.exception("%s/%s: unloadable job %s, deleting", self._path, self._name, job_id)
            self._db.execute("DELETE FROM jobs WHERE queue = ? AND id = ?", (self._name, job_id))
            self._db.commit()
            raise UnloadableJob(job_id, exc) from exc


def _sort_key(job: m.Job[Any]) -> SortKey:
    return SortKey(job.disable, job.run_at.astimezone(timezone.utc), job.id)


def _decode(job_id: str, blob: bytes) -> Any:
    job = pickle.loads(blob)  # nosemgrep: python.lang.security.deserialization.pickle.avoid-pickle
    missing = [f.name for f in fields(job) if not hasattr(job, f.name)]
    if missing:
        raise ValueError(f"job {job_id!r} is missing fields {missing}")
    return job
