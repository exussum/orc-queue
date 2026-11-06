import asyncio
import sqlite3
from datetime import timedelta

import pytest
from conftest import FlakyStore, at, noop, now

from orc_queue import Coordinator, Job, JobQueue, MemoryStore, ThreadLane


async def test_takes_earliest_due_across_queues_ties_by_queue_order(two_queues, freezegun_clock):
    freezegun_clock.move_to(at(10))
    a, b = two_queues
    with a.coordinator_lock:
        a.add(Job(at(9), noop, id="a1"))
    with a.coordinator_lock:
        a.add(Job(at(8), noop, id="y"))
    with b.coordinator_lock:
        b.add(Job(at(8), noop, id="x"))
    with b.coordinator_lock:
        b.add(Job(at(12), noop, id="b2"))
    ctl = Coordinator(ThreadLane(a, b))
    ctl.start()
    taken = [await ctl.next() for _ in range(3)]
    assert [(q.name, j.id) for q, j in taken] == [("a", "y"), ("b", "x"), ("a", "a1")]
    with a.coordinator_lock:
        assert a.count() == 0 and b.count() == 1


async def test_sleeps_until_due(two_queues):
    a, b = two_queues
    with b.coordinator_lock:
        b.add(Job(now() + timedelta(seconds=0.15), noop, id="soon"))
    ctl = Coordinator(ThreadLane(a, b))
    ctl.start()
    queue, job = await ctl.next()
    assert queue is b and job.id == "soon" and now() >= job.run_at


async def test_add_of_an_earlier_job_wakes_the_scan(two_queues):
    a, b = two_queues
    with a.coordinator_lock:
        a.add(Job(now() + timedelta(seconds=5), noop, id="far"))
    ctl = Coordinator(ThreadLane(a, b))
    ctl.start()
    task = asyncio.create_task(ctl.next())
    await asyncio.sleep(0.02)
    with b.coordinator_lock:
        b.add(Job(now(), noop, id="now"))
    queue, job = await asyncio.wait_for(task, 1)
    assert queue is b and job.id == "now"


async def test_skips_paused_queues_until_resumed(two_queues, freezegun_clock):
    freezegun_clock.move_to(at(10))
    a, b = two_queues
    with a.coordinator_lock:
        a.add(Job(at(1), noop, id="a1"))
    with a.coordinator_lock:
        a.pause()
    with b.coordinator_lock:
        b.add(Job(at(2), noop, id="b-disabled", disable=True))
    with b.coordinator_lock:
        b.add(Job(at(3), noop, id="b1"))
    ctl = Coordinator(ThreadLane(a, b))
    ctl.start()
    assert (await ctl.next())[1].id == "b1"
    task = asyncio.create_task(ctl.next())
    await asyncio.sleep(0.02)
    assert not task.done()
    with a.coordinator_lock:
        a.resume()
    assert (await asyncio.wait_for(task, 1))[1].id == "a1"


async def test_coordinator_finds_queues_by_name(two_queues):
    a, b = two_queues
    ctl = Coordinator(ThreadLane(a, b))
    ctl.start()
    assert ctl.lookup("a") is a and ctl.lookup("b") is b and ctl.queues == (a, b)
    assert ctl.lookup("c") is None


async def test_coordinator_finds_a_job_across_queues(two_queues):
    a, b = two_queues
    with b.coordinator_lock:
        b.add(Job(at(1), noop, id="x"))
    ctl = Coordinator(ThreadLane(a, b))
    ctl.start()
    queue, job = ctl.find("x")
    assert queue is b and job.id == "x"
    assert ctl.find("missing") is None


async def test_coordinator_cancels_and_pauses_a_job_across_queues(two_queues):
    a, b = two_queues
    with b.coordinator_lock:
        b.add(Job(at(1), noop, id="x"))
    ctl = Coordinator(ThreadLane(a, b))
    ctl.start()
    with b.coordinator_lock:
        assert ctl.pause_job("x", True) and b.get("x").disable
    with b.coordinator_lock:
        assert ctl.cancel_job("x") and b.count() == 0
    assert not ctl.pause_job("x", True) and not ctl.cancel_job("x")


async def test_next_after_stop_fails_fast(two_queues):
    ctl = Coordinator(ThreadLane(*two_queues))
    await ctl.stop()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(ctl.next(), 1)


async def test_a_store_error_during_a_scan_comes_out_of_next(lock, wake, events):
    q = JobQueue(lock, wake, FlakyStore(MemoryStore(), {"first"}), 100, events, name="q")
    with q.coordinator_lock:
        q.add(Job(at(1), noop, id="x"))
    ctl = Coordinator(ThreadLane(q))
    ctl.start()
    with pytest.raises(sqlite3.OperationalError, match="disk I/O error"):
        await asyncio.wait_for(ctl.next(), 2)
    with pytest.raises(RuntimeError, match="not running"):
        await ctl.next()


async def test_next_before_start_fails_fast(two_queues):
    with pytest.raises(RuntimeError, match="not running"):
        await Coordinator(ThreadLane(*two_queues)).next()
