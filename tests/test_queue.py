import logging
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from conftest import at, noop, now

from orc_queue import Event, Job, JobQueue, MemoryStore, on


def drain(queue):
    out = []
    with queue.coordinator_lock:
        while popped := queue.pop():
            out.append(popped.id)
    return out


def test_empty(queue):
    with queue.coordinator_lock:
        assert queue.next_job() is None
    with queue.coordinator_lock:
        assert queue.pop() is None
    with queue.coordinator_lock:
        assert queue.get("x") is None
    with queue.coordinator_lock:
        assert queue.count() == 0


def test_orders_by_time_then_id_across_timezones(queue):
    with queue.coordinator_lock:
        queue.add(Job(at(12), noop, id="b"))
    with queue.coordinator_lock:
        queue.add(Job(at(12), noop, id="a"))
    with queue.coordinator_lock:
        queue.add(Job(datetime(2026, 11, 1, 4, tzinfo=ZoneInfo("America/New_York")), noop, id="ny"))
    with queue.coordinator_lock:
        assert (queue.get("ny")).run_at.tzinfo == ZoneInfo("America/New_York")
    assert drain(queue) == ["ny", "a", "b"]


def test_add_replaces_existing_id(queue):
    with queue.coordinator_lock:
        queue.add(Job(at(5), noop, payload="old", id="a"))
    with queue.coordinator_lock:
        queue.add(Job(at(1), noop, id="b"))
    with queue.coordinator_lock:
        queue.add(Job(at(0), noop, payload="new", id="a"))
    with queue.coordinator_lock:
        assert queue.count() == 2
    with queue.coordinator_lock:
        assert (queue.get("a")).payload == "new"
    with queue.coordinator_lock:
        assert (queue.next_job()).id == "a"


def test_update_reorders_and_requires_existing(queue):
    a = Job(at(9), noop, id="a")
    with queue.coordinator_lock:
        queue.add(a)
    with queue.coordinator_lock:
        queue.add(Job(at(12), noop, id="b"))
    a.run_at = at(13)
    with queue.coordinator_lock:
        queue.update(a)
    with queue.coordinator_lock:
        assert (queue.next_job()).id == "b"
    with pytest.raises(KeyError):
        with queue.coordinator_lock:
            queue.update(Job(at(1), noop, id="missing"))


def test_get_remove_contains(queue):
    with queue.coordinator_lock:
        queue.add(Job(at(1), noop, payload={"k": 1}, id="a"))
    with queue.coordinator_lock:
        assert (queue.get("a")).payload == {"k": 1}
    with queue.coordinator_lock:
        assert queue.get("a") is not None
    with queue.coordinator_lock:
        assert (queue.remove("a")).id == "a"
    with queue.coordinator_lock:
        assert queue.get("a") is None
    with pytest.raises(KeyError):
        with queue.coordinator_lock:
            queue.remove("a")


def test_pause_blocks_pop_but_reports_next(queue):
    with queue.coordinator_lock:
        queue.add(Job(at(1), noop, id="a"))
    with queue.coordinator_lock:
        queue.pause()
    assert queue.paused and at(12) <= queue.paused_at <= now()
    with queue.coordinator_lock:
        assert (queue.next_job()).id == "a"
    with queue.coordinator_lock:
        assert queue.pop() is None
    with queue.coordinator_lock:
        queue.resume()
    assert not queue.paused_at
    with queue.coordinator_lock:
        assert queue.pop().id == "a"
    with queue.coordinator_lock:
        queue.pause()
    with queue.coordinator_lock:
        queue.reset()
    with queue.coordinator_lock:
        assert queue.paused and queue.count() == 0


def test_pop_declines_a_job_not_yet_due(queue, freezegun_clock):
    freezegun_clock.move_to(at(1))
    with queue.coordinator_lock:
        queue.add(Job(at(3), noop, id="later"))
    with queue.coordinator_lock:
        assert queue.pop() is None
    freezegun_clock.move_to(at(3))
    with queue.coordinator_lock:
        assert queue.pop().id == "later"


def test_next_job_deletes_the_disabled_jobs_whose_time_has_passed(queue, caplog, freezegun_clock):
    freezegun_clock.move_to(at(5))
    with queue.coordinator_lock:
        queue.add(Job(at(1), noop, id="p1", disable=True))
    with queue.coordinator_lock:
        queue.add(Job(at(2), noop, id="p2", disable=True))
    with queue.coordinator_lock:
        queue.add(Job(at(3), noop, id="live"))
    with queue.coordinator_lock:
        queue.add(Job(at(9), noop, id="p-later", disable=True))
    with queue.coordinator_lock:
        assert [j.id for j in queue.jobs()] == ["live", "p1", "p2", "p-later"]
    with caplog.at_level(logging.INFO, logger="orc_queue"):
        with queue.coordinator_lock:
            assert (queue.next_job()).id == "live"
    with queue.coordinator_lock:
        assert [j.id for j in queue.jobs()] == ["live", "p-later"]
    assert any("deleted skipped disabled jobs" in r.message and "p1" in r.message and "p2" in r.message for r in caplog.records)
    with queue.coordinator_lock:
        queue.pause()
    with queue.coordinator_lock:
        assert queue.pop() is None
    with queue.coordinator_lock:
        queue.resume()
    with queue.coordinator_lock:
        popped = queue.pop()
    assert popped.id == "live"
    with queue.coordinator_lock:
        assert queue.next_job() is None and queue.count() == 1
    freezegun_clock.move_to(at(10))
    with queue.coordinator_lock:
        assert queue.next_job() is None and queue.count() == 0


def test_dropping_a_skipped_disabled_job_emits_removed(queue, freezegun_clock):
    freezegun_clock.move_to(at(5))
    removed = []
    on(queue.events, Event.REMOVED, lambda queue, job: removed.append(job.id))
    with queue.coordinator_lock:
        queue.add(Job(at(1), noop, id="gone", disable=True))
    with queue.coordinator_lock:
        queue.add(Job(at(9), noop, id="kept", disable=True))
    with queue.coordinator_lock:
        assert queue.next_job() is None
    assert removed == ["gone"]


def test_a_disabled_job_waits_for_its_time_before_it_is_dropped(queue, freezegun_clock):
    freezegun_clock.move_to(at(5))
    with queue.coordinator_lock:
        queue.add(Job(at(1), noop, id="a", disable=True))
    with queue.coordinator_lock:
        queue.add(Job(at(9), noop, id="b", disable=True))
    with queue.coordinator_lock:
        assert queue.next_job() is None and [j.id for j in queue.jobs()] == ["b"]
    freezegun_clock.move_to(at(9))
    with queue.coordinator_lock:
        assert queue.next_job() is None and queue.count() == 0


def test_lock_is_shared_reentrant_and_blocks_threads(lock, wake, store, events):
    a = JobQueue(lock, wake, store, 100, events)
    b = JobQueue(lock, wake, MemoryStore(), 100, events)
    order = []

    def other():
        order.append("waiting")
        with b.coordinator_lock:
            b.add(Job(at(8), noop, id="y"))
        order.append("added")

    with a.coordinator_lock:
        thread = threading.Thread(target=other)
        thread.start()
        time.sleep(0.05)
        assert order == ["waiting"]
        with b.coordinator_lock:
            a.add(Job(at(1), noop, id="x"))
            assert a.pop().id == "x"
        order.append("exiting")
    thread.join()
    assert order == ["waiting", "exiting", "added"]
