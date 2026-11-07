import asyncio
import sqlite3
import threading
import time

import pytest
from conftest import FlakyStore, noop, now

from orc_queue import AsyncLane, Coordinator, Event, Job, JobQueue, MemoryStore, ThreadLane, ThreadService, on


def recorder(seen, label, sleep=0.0, block=None):
    def fn():
        seen.append(("start", label))
        if block is not None:
            block.wait(2)
        time.sleep(sleep)
        seen.append(("end", label))

    return fn


@pytest.mark.parametrize("kind", ["thread", "async"])
async def test_lanes_run_in_parallel_and_serial_within(two_queues, kind):
    a, b = two_queues
    seen = []
    release = threading.Event() if kind == "thread" else asyncio.Event()

    async def slow_async():
        seen.append(("start", "a1-slow"))
        await release.wait()
        seen.append(("end", "a1-slow"))

    base = now()
    with a.coordinator_lock:
        a.add(Job(base, recorder(seen, "a1-slow", block=release) if kind == "thread" else slow_async, id="a1-slow"))
    with a.coordinator_lock:
        a.add(Job(base, recorder(seen, "a2-next"), id="a2-next"))
    with b.coordinator_lock:
        b.add(Job(base, recorder(seen, "fast"), id="fast"))
    lane_a = ThreadLane(a) if kind == "thread" else AsyncLane(a)
    coordinator = Coordinator(lane_a, ThreadLane(b))
    coordinator.resume()
    coordinator.start()
    task = asyncio.create_task(coordinator.run())
    for _ in range(200):
        if ("end", "fast") in seen:
            break
        await asyncio.sleep(0.01)
    assert ("end", "fast") in seen and ("end", "a1-slow") not in seen
    release.set()
    for _ in range(300):
        if ("end", "a2-next") in seen:
            break
        await asyncio.sleep(0.01)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await coordinator.stop()
    assert [e for e in seen if e[1] != "fast"] == [("start", "a1-slow"), ("end", "a1-slow"), ("start", "a2-next"), ("end", "a2-next")]


async def test_lane_keeps_slot_order_across_its_queues(two_queues, make_lane, drive):
    a, b = two_queues
    seen = []
    base = now()
    for i, q in enumerate([a, b, a, b]):
        with q.coordinator_lock:
            q.add(Job(base, lambda payload: seen.append(payload), payload=f"j{i}", id=f"j{i}"))
    await drive(Coordinator(make_lane(a, b)), until=lambda: len(seen) == 4)
    assert seen == ["j0", "j2", "j1", "j3"]


async def test_job_exception_is_isolated(two_queues, make_lane, drive, caplog, events):
    a, b = two_queues
    done, errors = [], []

    def bad():
        raise RuntimeError("boom")

    base = now()
    with a.coordinator_lock:
        a.add(Job(base, bad, id="bad"))
    with a.coordinator_lock:
        a.add(Job(base, lambda payload: done.append(payload), payload="good", id="good"))
    on(events, Event.ERROR, lambda queue, job, error: errors.append((job.id, str(error))))
    lane = make_lane(a, b)
    with caplog.at_level("ERROR", logger="orc_queue"):
        await drive(Coordinator(lane), until=lambda: done == ["good"])
    assert errors == [("bad", "boom")]
    assert any("job bad on queue 'a' failed" in r.message for r in caplog.records)


async def test_stop_finishes_what_was_already_submitted(two_queues, make_lane):
    a, b = two_queues
    finished = []
    base = now()
    for i in range(3):
        with a.coordinator_lock:
            a.add(Job(base, recorder(finished, f"j{i}", sleep=0.05), id=f"j{i}"))
    coordinator = Coordinator(make_lane(a, b))
    coordinator.resume()
    coordinator.start()
    task = asyncio.create_task(coordinator.run())
    while True:
        with a.coordinator_lock:
            if not a.count():
                break
        await asyncio.sleep(0.01)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await coordinator.stop()
    assert [e[1] for e in finished if e[0] == "end"] == ["j0", "j1", "j2"]


async def test_shutdown_from_another_thread_stops_the_services_and_run_returns(two_queues):
    a, b = two_queues
    stopped = threading.Event()

    def service(stop):
        stop.wait()
        stopped.set()

    coordinator = Coordinator(ThreadLane(a, b), services=[ThreadService(service, name="svc")])
    coordinator.start()
    coordinator.resume()
    task = asyncio.create_task(coordinator.run())
    await asyncio.sleep(0.05)
    shutter = threading.Thread(target=coordinator.shutdown, args=(5,))
    shutter.start()
    assert await asyncio.wait_for(task, 5) is None
    while shutter.is_alive():
        await asyncio.sleep(0.01)
    assert stopped.is_set()


async def test_thread_lane_keeps_the_loop_responsive(two_queues):
    a, b = two_queues
    seen = []

    def fn():
        seen.append(threading.get_ident())
        time.sleep(0.2)

    with a.coordinator_lock:
        a.add(Job(now(), fn, id="x"))
    coordinator = Coordinator(ThreadLane(a, b))
    coordinator.resume()
    coordinator.start()
    task = asyncio.create_task(coordinator.run())
    while not seen:
        await asyncio.sleep(0.01)
    t = time.monotonic()
    await asyncio.sleep(0.01)
    assert time.monotonic() - t < 0.1
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await coordinator.stop()
    assert seen[0] != threading.get_ident()


async def test_async_lane_timeout_fails_the_job_and_continues(two_queues, drive, events):
    a, b = two_queues
    done, errors = [], []

    async def hang():
        await asyncio.sleep(10)

    base = now()
    with a.coordinator_lock:
        a.add(Job(base, hang, id="hang"))
    with a.coordinator_lock:
        a.add(Job(base, lambda payload: done.append(payload), payload="next", id="next"))
    on(events, Event.ERROR, lambda queue, job, error: errors.append((job.id, type(error).__name__)))
    lane = AsyncLane(a, b, timeout=0.05)
    await drive(Coordinator(lane), until=lambda: done == ["next"])
    assert errors == [("hang", "TimeoutError")]


async def test_lane_assignment_is_validated_and_stop_is_safe_before_run(two_queues):
    a, b = two_queues
    with pytest.raises(ValueError, match="two lanes"):
        Coordinator(ThreadLane(a, b), AsyncLane(b))
    with pytest.raises(ValueError, match="at least one"):
        AsyncLane()
    with pytest.raises(ValueError, match="at least one"):
        Coordinator()
    await Coordinator(ThreadLane(a), AsyncLane(b)).stop()
    assert noop() is None


async def test_fn_arguments_follow_its_signature(two_queues, make_lane, drive):
    a, b = two_queues
    calls = []
    context = object()

    def bare():
        calls.append(("bare",))

    def with_payload(payload):
        calls.append(("payload", payload))

    def with_ctx(*, ctx):
        calls.append(("ctx", ctx))

    def with_both(payload, ctx):
        calls.append(("both", payload, ctx))

    base = now()
    with a.coordinator_lock:
        a.add(Job(base, bare, id="1"))
    with a.coordinator_lock:
        a.add(Job(base, with_payload, "p", id="2"))
    with a.coordinator_lock:
        a.add(Job(base, with_ctx, id="3"))
    with a.coordinator_lock:
        a.add(Job(base, with_both, "d", id="4"))
    await drive(Coordinator(make_lane(a, b), context=context), until=lambda: len(calls) == 4)
    assert calls == [("bare",), ("payload", "p"), ("ctx", context), ("both", "d", context)]


async def test_coordinator_starts_paused_until_resumed(two_queues, drive):
    a, b = two_queues
    ran = []
    with a.coordinator_lock:
        a.add(Job(now(), lambda: ran.append(1), id="x"))
    coordinator = Coordinator(AsyncLane(a, b))
    assert not a.paused and not b.paused
    coordinator.start()
    task = asyncio.create_task(coordinator.run())
    await asyncio.sleep(0.05)
    assert ran == []
    coordinator.resume()
    for _ in range(100):
        if ran:
            break
        await asyncio.sleep(0.01)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await coordinator.stop()
    assert ran == [1]


async def test_a_store_error_during_a_pop_ends_run(lock, wake, events):
    q = JobQueue(lock, wake, FlakyStore(MemoryStore(), {"delete"}), 100, events, name="q")
    with q.coordinator_lock:
        q.add(Job(now(), noop, id="x"))
    coordinator = Coordinator(ThreadLane(q))
    coordinator.resume()
    with pytest.raises(sqlite3.OperationalError, match="disk I/O error"):
        coordinator.start()
        await asyncio.wait_for(coordinator.run(), 2)
    await coordinator.stop()


async def test_no_thread_runs_before_start_and_all_do_after(two_queues):
    a, b = two_queues
    service = ThreadService(lambda stop: stop.wait(), name="svc")
    coordinator = Coordinator(ThreadLane(a), ThreadLane(b), services=[service])
    before = set(threading.enumerate())
    assert not any(t.name in ("coordinator", "lane", "service-svc") for t in before)
    coordinator.start()
    started = sorted(t.name for t in threading.enumerate() if t not in before)
    assert started == ["coordinator", "lane", "lane", "service-svc"]
    await coordinator.stop()
    assert not any(t.name in ("coordinator", "lane", "service-svc") for t in threading.enumerate())
