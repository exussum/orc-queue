import asyncio
import sqlite3
import threading
import time
from datetime import datetime, timezone

import freezegun
import pytest
from freezegun import freeze_time

from orc_queue import AsyncLane, AsyncService, Events, JobQueue, MemoryStore, SqliteStore, ThreadLane, ThreadService

freezegun.configure(extend_ignore_list=["asyncio", "concurrent", "selectors", "pytest_asyncio"])


def at(hour: int) -> datetime:
    return datetime(2026, 11, 1, hour, tzinfo=timezone.utc)


def now() -> datetime:
    return datetime.now(timezone.utc)


def noop():
    pass


@pytest.fixture(autouse=True)
def freezegun_clock():
    with freeze_time(at(12), tick=True) as freezegun_clock:
        yield freezegun_clock


@pytest.fixture
def lock():
    return threading.RLock()


@pytest.fixture
def wake():
    return threading.Event()


@pytest.fixture
def events():
    return Events()


@pytest.fixture(params=["memory", "sqlite"])
def store(request, tmp_path):
    if request.param == "memory":
        yield MemoryStore()
        return
    s = SqliteStore(str(tmp_path / "q.db"))
    yield s
    s.close()


@pytest.fixture
def queue(lock, wake, store, events):
    return JobQueue(lock, wake, store, 100, events)


@pytest.fixture
def two_queues(lock, wake, events):
    return JobQueue(lock, wake, MemoryStore(), 100, events, name="a"), JobQueue(lock, wake, MemoryStore(), 100, events, name="b")


@pytest.fixture
def drive():
    async def _drive(coordinator, until=None, seconds=0.0, timeout=3.0):
        coordinator.start()
        coordinator.resume()
        task = asyncio.create_task(coordinator.run())
        try:
            if until is None:
                await asyncio.sleep(seconds)
            else:
                deadline = time.monotonic() + timeout
                while not until() and time.monotonic() < deadline:
                    await asyncio.sleep(0.01)
                assert until(), "condition not met before timeout"
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await coordinator.stop()

    return _drive


@pytest.fixture(params=["thread", "async"])
def make_lane(request):
    def make(*queues, **kw):
        return ThreadLane(*queues, **kw) if request.param == "thread" else AsyncLane(*queues, **kw)

    make.kind = request.param
    return make


@pytest.fixture(params=["thread", "async"])
def make_service(request):
    def make(crashes=0, **kw):
        runs = []

        def body():
            runs.append(time.monotonic())
            if len(runs) <= crashes:
                raise RuntimeError("boom")

        if request.param == "thread":

            def svc(stop):
                body()
                stop.wait()

            return ThreadService(svc, **kw), runs

        async def asvc(stop):
            body()
            await stop.wait()

        return AsyncService(asvc, **kw), runs

    make.kind = request.param
    return make


class FlakyStore:
    def __init__(self, store, fail_on):
        self._store = store
        self.fail_on = set(fail_on)

    def __getattr__(self, name):
        attr = getattr(self._store, name)
        if name not in self.fail_on:
            return attr

        def once(*args, **kwargs):
            self.fail_on.discard(name)
            raise sqlite3.OperationalError("disk I/O error")

        return once
