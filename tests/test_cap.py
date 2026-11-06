import logging
import math

import pytest
from conftest import at, noop

from orc_queue import CronJob, Job, JobQueue, OnFull


@pytest.fixture(params=[OnFull.REJECT, OnFull.EVICT])
def capped(request, lock, wake, store, events):
    return JobQueue(lock, wake, store, 2, events, name="capped", on_full=request.param)


def test_replacing_existing_id_never_counts_against_cap(capped):
    with capped.coordinator_lock:
        capped.add(Job(at(1), noop, id="a"))
    with capped.coordinator_lock:
        capped.add(Job(at(2), noop, id="b"))
    with capped.coordinator_lock:
        assert capped.add(Job(at(5), noop, payload="new", id="a")) is True
    with capped.coordinator_lock:
        assert sorted(j.id for j in capped.jobs()) == ["a", "b"]
    with capped.coordinator_lock:
        assert (capped.get("a")).payload == "new"


def test_cron_respawn_is_not_blocked_by_cap(capped, freezegun_clock):
    freezegun_clock.move_to(at(1))
    with capped.coordinator_lock:
        capped.add(CronJob(at(1), noop, cron="0 * * * *", id="c"))
    with capped.coordinator_lock:
        capped.add(Job(at(9), noop, id="x"))
    with capped.coordinator_lock:
        popped = capped.pop()
        assert popped.id == "c" and capped.next_job().run_at == at(2)
    with capped.coordinator_lock:
        assert capped.count() == 2


def test_reject_returns_false_until_room_returns(lock, wake, store, caplog, events):
    q = JobQueue(lock, wake, store, 2, events, name="capped")
    with q.coordinator_lock:
        q.add(Job(at(1), noop, id="a"))
    with q.coordinator_lock:
        q.add(Job(at(2), noop, id="b"))
    with caplog.at_level(logging.ERROR, logger="orc_queue"):
        with q.coordinator_lock:
            assert q.add(Job(at(3), noop, id="c")) is False
    assert any("queue 'capped' is full (2/2), rejected job c" in r.message for r in caplog.records)
    with q.coordinator_lock:
        q.pop()
    with q.coordinator_lock:
        assert q.add(Job(at(3), noop, id="c")) is True
    with q.coordinator_lock:
        q.remove("b")
    with q.coordinator_lock:
        assert q.add(Job(at(4), noop, id="d")) is True


def test_evict_drops_earliest_scheduled(lock, wake, store, caplog, events):
    q = JobQueue(lock, wake, store, 2, events, on_full=OnFull.EVICT)
    with q.coordinator_lock:
        q.add(Job(at(9), noop, id="late"))
    with q.coordinator_lock:
        q.add(Job(at(1), noop, id="early"))
    with caplog.at_level(logging.WARNING, logger="orc_queue"):
        with q.coordinator_lock:
            assert q.add(Job(at(5), noop, id="mid")) is True
    with q.coordinator_lock:
        assert [j.id for j in q.jobs()] == ["mid", "late"]
    assert any("evicted earliest job early for mid" in r.message for r in caplog.records)


def test_cron_jobs_are_never_evicted(lock, wake, store, caplog, events):
    q = JobQueue(lock, wake, store, 2, events, on_full=OnFull.EVICT)
    with q.coordinator_lock:
        q.add(CronJob(at(1), noop, cron="0 * * * *", id="c1"))
    with q.coordinator_lock:
        q.add(Job(at(5), noop, id="x"))
    with q.coordinator_lock:
        assert q.add(Job(at(6), noop, id="y")) is True
    with q.coordinator_lock:
        assert sorted(j.id for j in q.jobs()) == ["c1", "y"]
    with q.coordinator_lock:
        q.add(CronJob(at(2), noop, cron="0 * * * *", id="c2"))
    with caplog.at_level(logging.ERROR, logger="orc_queue"):
        with q.coordinator_lock:
            assert q.add(Job(at(3), noop, id="z")) is False
    with q.coordinator_lock:
        assert sorted(j.id for j in q.jobs()) == ["c1", "c2"]


def test_cap_zero_means_unlimited(lock, wake, store, events):
    q = JobQueue(lock, wake, store, 0, events)
    assert q.cap == math.inf
    for i in range(50):
        with q.coordinator_lock:
            assert q.add(Job(at(1), noop, id=str(i))) is True
    with q.coordinator_lock:
        assert q.count() == 50
