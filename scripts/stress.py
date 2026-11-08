"""Stress run: ten capped queues on thread and async lanes, jobs that sleep 0-2s, producers that
overrun capacity, random pauses, cron chains, and services that crash and restart.

usage: uv run python scripts/stress.py --duration 1800 --db /tmp/stress.db
"""

import argparse
import asyncio
import collections
import logging
import random
import sys
import threading
import time
from datetime import timedelta, timezone
from pathlib import Path
from typing import cast

from orc_queue import (
    AsyncLane,
    AsyncService,
    Coordinator,
    CronJob,
    Events,
    Job,
    JobQueue,
    MemoryStore,
    OnFull,
    SqliteStore,
    ThreadLane,
    ThreadService,
    aware,
    schedule,
    utcnow,
)

STATS_LOCK = threading.Lock()
RUNS: list[tuple[str, str, float, float]] = []  # job id, queue name, start, end (monotonic)
RUN_IDS: collections.Counter = collections.Counter()
EARLY: list[str] = []
CRASHES: collections.Counter = collections.Counter()
FAILURES: list[str] = []
LOG_COUNTS: collections.Counter = collections.Counter()
UNEXPECTED: list[str] = []


class CountingHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        msg = record.getMessage()
        for key in (
            "evicted earliest job",
            "rejected job",
            "giving up",
            "restarting in",
            "deleted skipped disabled jobs",
            "exited on its own",
            "crashed",
        ):
            if key in msg:
                LOG_COUNTS[key] += 1
        if record.levelno >= logging.ERROR and not any(k in msg for k in ("crashed", "failed", "rejected job")):
            LOG_COUNTS["unexpected-error"] += 1
            UNEXPECTED.append(msg)


def fail(msg: str) -> None:
    with STATS_LOCK:
        FAILURES.append(msg)
    print("FAIL", msg, flush=True)


def _record(payload: dict, start: float) -> None:
    with STATS_LOCK:
        RUNS.append((payload["id"], payload["queue"], start, time.monotonic()))
        RUN_IDS[payload["id"]] += 1
        if RUN_IDS[payload["id"]] > 1 and not payload.get("cron"):
            FAILURES.append(f"{payload['id']} ran twice")
    if utcnow() < payload["run_at"]:
        EARLY.append(payload["id"])


def nap(payload: dict) -> None:
    start = time.monotonic()
    time.sleep(payload["sleep"])
    if payload.get("raise"):
        _record(payload, start)
        raise RuntimeError("planned")
    _record(payload, start)


async def nap_async(payload: dict) -> None:
    start = time.monotonic()
    await asyncio.sleep(payload["sleep"])
    if payload.get("raise"):
        _record(payload, start)
        raise RuntimeError("planned")
    _record(payload, start)


def tick(payload: dict) -> None:
    _record(payload, time.monotonic())


# ---- services that crash now and then ----


def flaky_thread(stop: threading.Event) -> None:
    rng = random.Random()
    while not stop.is_set():
        if stop.wait(rng.uniform(5, 20)):
            return
        CRASHES["thread"] += 1
        raise RuntimeError("thread service hiccup")


async def flaky_async(stop: asyncio.Event) -> None:
    rng = random.Random()
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), rng.uniform(5, 20))
            return
        except TimeoutError:
            CRASHES["async"] += 1
            raise RuntimeError("async service hiccup") from None


# ---- load ----


async def producer(
    tag: str, queues: list[tuple[JobQueue, str]], stop: asyncio.Event, rng: random.Random, stats: collections.Counter, horizon: float
) -> None:
    seq = 0
    while not stop.is_set():
        queue, kind = rng.choice(queues)
        seq += 1
        jid = f"{queue.name}-{tag}{seq}"
        run_at = aware(utcnow() + timedelta(seconds=rng.uniform(0, horizon)))
        payload = {"id": jid, "queue": queue.name, "sleep": rng.uniform(0, 2), "run_at": run_at, "raise": rng.random() < 0.03}
        fn = nap_async if kind == "async" else nap
        with queue.coordinator_lock:
            added = queue.add(Job(run_at, None, fn, payload, id=jid))
        if added:
            stats["added"] += 1
        else:
            stats["rejected"] += 1
        await asyncio.sleep(rng.expovariate(1.5))


async def pauser(queues: list[JobQueue], stop: asyncio.Event, rng: random.Random, stats: collections.Counter) -> None:
    while not stop.is_set():
        q = rng.choice(queues)
        with q.coordinator_lock:
            q.pause()
        stats["pause"] += 1
        await asyncio.sleep(rng.uniform(0.5, 4))
        with q.coordinator_lock:
            q.resume()
        stats["resume"] += 1
        await asyncio.sleep(rng.uniform(1, 5))


async def heartbeat(stop: asyncio.Event, lag: list[float]) -> None:
    while not stop.is_set():
        t = time.monotonic()
        await asyncio.sleep(0.02)
        lag[0] = max(lag[0], time.monotonic() - t - 0.02)


async def progress(
    queues: list[JobQueue], services: list, stop: asyncio.Event, start: float, duration: float, stats: collections.Counter, lag: list[float]
) -> None:
    while not stop.is_set():
        await asyncio.sleep(30)
        with queues[0].coordinator_lock:
            sizes = " ".join(f"{q.name}={q.count()}{'P' if q.paused else ''}" for q in queues)
        restarts = sum(s.restarts for s in services)
        with STATS_LOCK:
            ran = len(RUNS)
        print(
            f"[{(time.monotonic() - start) / 60:5.1f}/{duration / 60:.0f}m] added={stats['added']} rejected={stats['rejected']} "
            f"evicted={LOG_COUNTS['evicted earliest job']} ran={ran} restarts={restarts} fails={len(FAILURES)} "
            f"lag={lag[0] * 1000:.0f}ms {sizes}",
            flush=True,
        )


def check(queues: list[JobQueue], lanes: list, services: list) -> None:
    if EARLY:
        fail(f"{len(EARLY)} jobs ran before their slot, e.g. {EARLY[:3]}")
    for q in queues:
        with q.coordinator_lock:
            if q.count() > q.cap:
                fail(f"{q.name} holds {q.count()} > cap {q.cap}")
    # thread lanes never overlap jobs from their queues
    for lane in lanes:
        if not isinstance(lane, ThreadLane):
            continue
        names = {q.name for q in lane.queues}
        runs = sorted((r for r in RUNS if r[1] in names), key=lambda r: r[2])
        for (_, _, _s1, e1), (_, _, s2, _) in zip(runs, runs[1:], strict=False):
            if s2 < e1 - 1e-3:
                fail(f"thread lane {sorted(names)} overlapped jobs")
                break
    for s in services:
        if s.restarts == 0:
            fail(f"service {s.name} never restarted")
    if LOG_COUNTS["giving up"]:
        fail(f"{LOG_COUNTS['giving up']} service(s) gave up")
    if LOG_COUNTS["unexpected-error"]:
        fail(f"{LOG_COUNTS['unexpected-error']} unexpected error log lines")


class CountingLock:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.acquires = 0
        self.contended = 0

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        self.acquires += 1
        if self._lock.acquire(blocking=False):
            return True
        self.contended += 1
        return self._lock.acquire(blocking, timeout)

    def release(self) -> None:
        self._lock.release()

    def _is_owned(self) -> bool:
        return self._lock._is_owned()  # type: ignore[attr-defined]

    def __enter__(self) -> bool:
        return self.acquire()

    def __exit__(self, *exc: object) -> None:
        self.release()


async def main(duration: float, db: str, seed: int, producers: int, horizon: float) -> int:
    logging.getLogger("orc_queue").addHandler(CountingHandler())
    logging.getLogger("orc_queue").setLevel(logging.INFO)
    rng = random.Random(seed)
    Path(db).unlink(missing_ok=True)
    counting = CountingLock()
    lock = cast(threading.RLock, counting)
    wake = threading.Event()
    events = Events()
    queues: list[JobQueue] = []
    for i in range(10):
        store = MemoryStore() if i % 2 == 0 else SqliteStore(db, f"q{i}")
        policy = OnFull.EVICT if i % 3 == 0 else OnFull.REJECT
        queues.append(JobQueue(lock, wake, store, rng.choice([10, 15, 20, 40]), events, name=f"q{i}", on_full=policy))
    lanes = [
        ThreadLane(queues[0]),
        ThreadLane(queues[1], queues[2]),
        ThreadLane(queues[3]),
        AsyncLane(queues[4]),
        AsyncLane(queues[5], queues[6], timeout=30),
        AsyncLane(queues[7], queues[8], queues[9]),
    ]
    kind_of = {}
    for lane in lanes:
        for q in lane.queues:
            kind_of[q.name] = "async" if isinstance(lane, AsyncLane) else "thread"
    services = [
        ThreadService(flaky_thread, name="t1"),
        ThreadService(flaky_thread, name="t2"),
        AsyncService(flaky_async, name="a1"),
        AsyncService(flaky_async, name="a2"),
    ]
    for n, q in enumerate(queues[:4]):
        payload = {"id": f"cron{n}", "queue": q.name, "run_at": utcnow(), "sleep": 0, "cron": True}
        with q.coordinator_lock:
            q.add(schedule("* * * * *", timezone.utc, tick, id_template=f"cron{n}-{{uuid}}", payload=payload))
    coordinator = Coordinator(*lanes, services=services)
    stats: collections.Counter = collections.Counter()
    lag = [0.0]
    stop = asyncio.Event()
    start = time.monotonic()
    producer_queues = [(q, kind_of[q.name]) for q in queues]
    coordinator.start()
    coordinator.resume()
    tasks = [
        asyncio.create_task(coordinator.run()),
        *(
            asyncio.create_task(producer(f"p{i}", producer_queues, stop, random.Random(seed + 1 + i), stats, horizon))
            for i in range(producers)
        ),
        asyncio.create_task(pauser(queues, stop, random.Random(seed + 2), stats)),
        asyncio.create_task(heartbeat(stop, lag)),
        asyncio.create_task(progress(queues, services, stop, start, duration, stats, lag)),
    ]
    await asyncio.sleep(duration)
    stop.set()
    for t in tasks[1:]:
        t.cancel()
    await asyncio.gather(*tasks[1:], return_exceptions=True)
    for q in queues:
        with q.coordinator_lock:
            q.resume()
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        with lock:
            if not any(not isinstance(j, CronJob) for q in queues for j in q.jobs()):
                break
        await asyncio.sleep(0.2)
    tasks[0].cancel()
    await asyncio.gather(tasks[0], return_exceptions=True)
    await coordinator.stop()
    check(queues, lanes, services)
    with STATS_LOCK:
        ran = len(RUNS)
    print("\n==== stress summary ====")
    print(f"duration   {duration / 60:.1f} min   seed {seed}")
    print(f"jobs       added {stats['added']}  rejected {stats['rejected']}  evicted {LOG_COUNTS['evicted earliest job']}  ran {ran}")
    print(f"cron runs  {sum(1 for r in RUNS if r[0].startswith('cron'))}")
    print(f"pauses     {stats['pause']}  skipped-disabled deletions {LOG_COUNTS['deleted skipped disabled jobs']}")
    print(f"services   crashes {dict(CRASHES)}  restarts {[(s.name, s.restarts) for s in services]}  gave up {LOG_COUNTS['giving up']}")
    print(f"loop lag   max {lag[0] * 1000:.0f}ms")
    print(f"lock       acquires {counting.acquires}  contended {counting.contended}")
    print(f"failures   {len(FAILURES)}")
    for f in FAILURES[:20]:
        print("  ", f)
    for m in UNEXPECTED[:10]:
        print("   unexpected:", m)
    return 1 if FAILURES else 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=60)
    ap.add_argument("--db", default="stress.db")
    ap.add_argument("--seed", type=int, default=int(time.time()))
    ap.add_argument("--producers", type=int, default=2)
    ap.add_argument("--horizon", type=float, default=60)
    a = ap.parse_args()
    sys.exit(asyncio.run(main(a.duration, a.db, a.seed, a.producers, a.horizon)))
