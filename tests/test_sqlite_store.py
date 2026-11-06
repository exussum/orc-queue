import logging
import pickle
import sqlite3
import sys
import types
from datetime import timedelta

import pytest
from conftest import at, noop

from orc_queue import Coordinator, CronJob, Job, JobQueue, SqliteStore, ThreadLane, UnloadableJob


@pytest.fixture
def db(tmp_path):
    return str(tmp_path / "q.db")


@pytest.fixture
def open_queue(lock, wake, db, events):
    stores = []

    def _open(name="default"):
        store = SqliteStore(db, name, discard_after=timedelta(days=1))
        stores.append(store)
        return JobQueue(lock, wake, store, 100, events)

    yield _open
    for store in stores:
        store.close()


def plant(db, job_id, blob, queue="default"):
    conn = sqlite3.connect(db)
    conn.execute("UPDATE jobs SET job = ? WHERE queue = ? AND id = ?", (blob, queue, job_id))
    conn.commit()
    conn.close()


def test_persists_across_instances_and_isolates_names(open_queue):
    q = open_queue("p")
    with q.coordinator_lock:
        q.add(Job(at(5), noop, id="late"))
    with q.coordinator_lock:
        q.add(Job(at(1), noop, id="z", disable=True))
    with q.coordinator_lock:
        q.add(Job(at(4), noop, id="early"))
    other = open_queue("other")
    with other.coordinator_lock:
        other.add(Job(at(0), noop, id="early"))
    reopened = open_queue("p")
    with reopened.coordinator_lock:
        assert [j.id for j in reopened.jobs()] == ["early", "late", "z"]
    with reopened.coordinator_lock:
        assert (reopened.get("z")).disable
    with reopened.coordinator_lock:
        assert (reopened.next_job()).run_at == at(4)


def test_startup_quietly_drops_unloadable_rows_per_queue_name(open_queue, db, caplog):
    q = open_queue()
    for jid in ("corrupt", "gone_class", "missing_field", "good"):
        with q.coordinator_lock:
            q.add(Job(at(1 if jid != "good" else 2), noop, id=jid))
    other = open_queue("other")
    with other.coordinator_lock:
        other.add(Job(at(1), noop, id="corrupt"))
    plant(db, "corrupt", b"garbage")

    mod = types.ModuleType("vanished_plugin")

    class Payload:
        pass

    Payload.__module__, Payload.__qualname__ = "vanished_plugin", "Payload"
    mod.Payload = Payload
    sys.modules["vanished_plugin"] = mod
    plant(db, "gone_class", pickle.dumps(Job(at(1), noop, payload=Payload(), id="gone_class")))
    del sys.modules["vanished_plugin"]

    old = Job(at(1), noop, id="missing_field")
    del old.__dict__["run_at"]
    plant(db, "missing_field", pickle.dumps(old))

    with caplog.at_level(logging.DEBUG, logger="orc_queue"):
        reopened = open_queue()
        with reopened.coordinator_lock:
            assert (reopened.next_job()).id == "good"
    with reopened.coordinator_lock:
        assert reopened.count() == 1
    assert max(r.levelno for r in caplog.records) <= logging.INFO
    msgs = [r.message for r in caplog.records]
    assert any("startup dropped job gone_class" in m and "vanished_plugin" in m for m in msgs)
    assert any("startup dropped job missing_field" in m and "missing fields ['run_at']" in m for m in msgs)
    assert any("dropped 3 unloadable job(s) at startup" in m for m in msgs)
    other = open_queue("other")
    with other.coordinator_lock:
        assert other.get("corrupt") is not None


def test_after_startup_an_unloadable_row_is_deleted_loudly(open_queue, db, caplog):
    q = open_queue()
    with q.coordinator_lock:
        q.add(Job(at(1), noop, id="bad"))
    with q.coordinator_lock:
        q.add(Job(at(2), noop, id="good"))
    plant(db, "bad", b"garbage")
    with caplog.at_level(logging.ERROR, logger="orc_queue"):
        with pytest.raises(UnloadableJob) as info:
            with q.coordinator_lock:
                q.next_job()
    assert info.value.job_id == "bad"
    assert any("unloadable job bad, deleting" in r.message and r.exc_info for r in caplog.records)
    with q.coordinator_lock:
        assert [j.id for j in q.jobs()] == ["good"]


def test_startup_discards_jobs_later_than_the_grace(lock, wake, db, caplog, events):
    q = JobQueue(lock, wake, SqliteStore(db), 100, events)
    with q.coordinator_lock:
        q.add(Job(at(1), noop, id="hours-late"))
    with q.coordinator_lock:
        q.add(Job(at(12) - timedelta(minutes=4), noop, id="just-late"))
    with q.coordinator_lock:
        q.add(Job(at(12) - timedelta(minutes=6), noop, id="too-late", disable=True))
    with q.coordinator_lock:
        q.add(Job(at(13), noop, id="future"))
    with q.coordinator_lock:
        q.add(CronJob(at(1), noop, cron="0 * * * *", id="cron-late"))
    with caplog.at_level(logging.INFO, logger="orc_queue"):
        reopened = JobQueue(lock, wake, SqliteStore(db, discard_after=timedelta(minutes=5)), 100, events)
    with reopened.coordinator_lock:
        assert sorted(j.id for j in reopened.jobs()) == ["cron-late", "future", "just-late"]
    assert any("discarded 2 job(s) more than 0:05:00 late at startup" in r.message for r in caplog.records)


def test_a_jobs_file_without_the_cron_column_is_migrated(lock, wake, db, events):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "CREATE TABLE jobs (queue TEXT NOT NULL, id TEXT NOT NULL, disable INTEGER NOT NULL,"
            " run_at TEXT NOT NULL, job BLOB NOT NULL, PRIMARY KEY (queue, id))"
        )
    q = JobQueue(lock, wake, SqliteStore(db), 100, events)
    with q.coordinator_lock:
        q.add(CronJob(at(13), noop, cron="0 * * * *", id="c"))
    with q.coordinator_lock:
        assert q.count() == 1


async def test_an_unloadable_row_does_not_stop_the_scan(open_queue, db):
    q = open_queue()
    with q.coordinator_lock:
        q.add(Job(at(1), noop, id="bad"))
    with q.coordinator_lock:
        q.add(Job(at(2), noop, id="good"))
    plant(db, "bad", b"garbage")
    coordinator = Coordinator([ThreadLane([q])])
    coordinator.start()
    queue, job = await coordinator.next()
    assert queue is q and job.id == "good"
