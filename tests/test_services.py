import asyncio
import logging
import math
import threading

import pytest
from conftest import at

from orc_queue import AsyncLane, AsyncService, Coordinator, Job, JobQueue, MemoryStore, ServiceFailed, ThreadService


@pytest.fixture
def coordinator_for(lock, wake, events):
    q = JobQueue(lock, wake, MemoryStore(), 100, events)
    return lambda *services: Coordinator([AsyncLane([q])], services=services)


async def test_runs_until_stopped_and_restarts_after_crashes(coordinator_for, make_service, drive, caplog):
    service, runs = make_service(crashes=2, name="svc", restart_after=0.02)
    with caplog.at_level(logging.WARNING, logger="orc_queue"):
        await drive(coordinator_for(service), seconds=0.3)
    assert len(runs) == 3 and service.restarts == 2
    assert sum("svc restarting in" in r.message for r in caplog.records) == 2


async def test_three_fast_failures_raise_out_of_the_coordinator(coordinator_for, make_service, caplog):
    service, runs = make_service(crashes=99, name="broken", restart_after=0.02)
    coordinator = coordinator_for(service)
    with caplog.at_level(logging.ERROR, logger="orc_queue"):
        with pytest.raises(ServiceFailed) as info:
            coordinator.start()
            await asyncio.wait_for(coordinator.run(), 2)
    assert info.value.service is service and isinstance(info.value.__cause__, RuntimeError)
    assert len(runs) == 3 and service.restarts == 2
    assert any("broken failed 3 times in a row, giving up" in r.message for r in caplog.records)
    await coordinator.stop()


async def test_a_long_run_resets_the_fast_count(coordinator_for, drive):
    runs = []

    async def svc(stop):
        runs.append(1)
        if len(runs) in (1, 2):
            raise RuntimeError("fast")
        await asyncio.sleep(0.05)
        raise RuntimeError("slow")

    service = AsyncService(svc, restart_after=0.01)
    await drive(coordinator_for(service), seconds=0.4)
    assert len(runs) >= 5


async def test_early_return_is_restarted_too(coordinator_for, drive):
    runs = []

    async def svc(stop):
        runs.append(1)
        if len(runs) == 1:
            return
        await stop.wait()

    service = AsyncService(svc, restart_after=0.01)
    await drive(coordinator_for(service), seconds=0.1)
    assert len(runs) == 2


async def test_no_restart_leaves_the_service_down_and_keeps_the_coordinator(coordinator_for, make_service):
    service, runs = make_service(crashes=99, restart_after=0)
    coordinator = coordinator_for(service)
    coordinator.resume()
    coordinator.start()
    task = asyncio.create_task(coordinator.run())
    await asyncio.sleep(0.1)
    assert not task.done() and len(runs) == 1 and service.restart_after == math.inf
    assert service.restarts == 0
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await coordinator.stop()


async def test_services_start_before_the_scan_and_stop_after_lanes(lock, wake, drive, events):
    order = []
    q = JobQueue(lock, wake, MemoryStore(), 100, events)

    async def svc(stop):
        order.append("service-start")
        await stop.wait()
        order.append("service-stop")

    async def job(payload):
        await asyncio.sleep(0.1)
        order.append(payload)

    with q.coordinator_lock:
        q.add(Job(at(0), job, payload="job", id="j"))
    coordinator = Coordinator([AsyncLane([q])], services=[AsyncService(svc)])
    await drive(coordinator, seconds=0.02)
    assert order == ["service-start", "job", "service-stop"]


async def test_services_that_ignore_stop(coordinator_for, drive, caplog):
    release = threading.Event()

    async def deaf(stop):
        await asyncio.sleep(100)

    def stuck(stop):
        release.wait(5)

    a = AsyncService(deaf, name="deaf", stop_timeout=0.05)
    b = ThreadService(stuck, name="stuck", stop_timeout=0.05)
    with caplog.at_level(logging.WARNING, logger="orc_queue"):
        await drive(coordinator_for(a, b), seconds=0.02)
    release.set()
    msgs = [r.message for r in caplog.records]
    assert any("deaf ignored stop" in m and "cancelling" in m for m in msgs)
    assert any("stuck ignored stop" in m and "abandoned" in m for m in msgs)
