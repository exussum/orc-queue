from __future__ import annotations

import abc
import asyncio
import contextlib
import functools
import inspect
import logging
import math
import threading
import time
import uuid
from dataclasses import replace
from datetime import tzinfo
from typing import Any, Awaitable, Callable, Generic, Iterable, Optional, ParamSpec, Sequence, TypeVar

from cronsim import CronSim

from . import model as m
from .dal import Store, UnloadableJob, expired

T = TypeVar("T")


P = ParamSpec("P")


R = TypeVar("R")


C = TypeVar("C", bound=type)


GIVE_UP_AFTER = 3


_log = logging.getLogger("orc_queue")


def held(cls: C) -> C:
    def check(method: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(method)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            if not args[0].coordinator_lock._is_owned():  # type: ignore[attr-defined]
                raise RuntimeError(f"{method.__name__} called without holding the lock")
            return method(*args, **kwargs)

        return wrapper

    for name, member in list(vars(cls).items()):
        if inspect.isfunction(member) and not name.startswith("__"):
            setattr(cls, name, check(member))
    return cls


@held
class JobQueue(Generic[T]):
    def __init__(
        self,
        coordinator_lock: threading.RLock,
        changed: threading.Event,
        store: Store[T],
        cap: float,
        events: m.Events,
        name: Optional[str] = None,
        on_full: m.OnFull = m.OnFull.REJECT,
    ) -> None:
        if cap < 0:
            raise ValueError("cap must be at least 0")
        self._store = store
        self._paused = False
        self._paused_at: Optional[m.AwareDatetime] = None
        self.coordinator_lock = coordinator_lock
        self.changed = changed
        self.events = events
        self.name = name
        self.cap = cap or math.inf
        self.on_full = on_full

    def add(self, job: m.AnyJob[T]) -> bool:
        new = not self._store.get(job.id)
        if new:
            size = self._store.count()
            if size >= self.cap and not (self.on_full == m.OnFull.EVICT and self._evict(job)):
                _log.error("queue %r is full (%d/%s), rejected job %s", self.name, size, self.cap, job.id)
                return False
        self._store.put(job)
        self.changed.set()
        emit(self.events, m.Event.ADDED if new else m.Event.MODIFIED, queue=self, job=job)
        return True

    def _evict(self, incoming: m.AnyJob[T]) -> bool:
        victim = next((j for j in self._store.all() if evictable(j)), None)
        if not victim:
            return False
        self._store.delete(victim.id)
        _log.warning("queue %r is full (%s), evicted earliest job %s for %s", self.name, self.cap, victim.id, incoming.id)
        emit(self.events, m.Event.EVICTED, queue=self, job=victim)
        return True

    def update(self, job: m.AnyJob[T]) -> None:
        if not self._store.get(job.id):
            raise KeyError(job.id)
        self._store.put(job)
        self.changed.set()
        emit(self.events, m.Event.MODIFIED, queue=self, job=job)

    def get(self, job_id: str) -> Optional[m.AnyJob[T]]:
        return self._store.get(job_id)

    def remove(self, job_id: str) -> m.AnyJob[T]:
        job = self._store.get(job_id)
        if not job:
            raise KeyError(job_id)
        self._store.delete(job_id)
        self.changed.set()
        emit(self.events, m.Event.CANCELED, queue=self, job=job)
        return job

    def pop(self) -> Optional[m.AnyJob[T]]:
        job = self.next_job()
        if not job or self._paused or job.run_at > m.utcnow():
            return None
        self._iterate(job, m.utcnow())
        return job

    def next_job(self) -> Optional[m.AnyJob[T]]:
        now = m.utcnow()
        dropped = []
        for job in self._store.droppable(now):
            if not self._iterate(job, now):
                dropped.append(job.id)
                emit(self.events, m.Event.EXPIRED if expired(job, now) else m.Event.SKIPPED, queue=self, job=job)
        if dropped:
            _log.info("queue %r: deleted jobs that will not run: %s", self.name, ", ".join(dropped))
        return self._store.first()

    def _iterate(self, job: m.AnyJob[T], now: m.AwareDatetime) -> Optional[m.AnyJob[T]]:
        self._store.delete(job.id)
        successor = next_instance(job, after=now)
        if successor:
            self._store.put(successor)
        return successor

    def jobs(self) -> list[m.AnyJob[T]]:
        return self._store.all()

    def reset(self) -> None:
        for job in self._store.all():
            if not isinstance(job, m.CronJob):
                self._store.delete(job.id)
        self.changed.set()
        emit(self.events, m.Event.RESET, queue=self)

    def pause(self) -> None:
        self._paused = True
        self._paused_at = m.utcnow()
        emit(self.events, m.Event.PAUSED, queue=self)

    def resume(self) -> None:
        self._paused = False
        self._paused_at = None
        self.changed.set()
        emit(self.events, m.Event.RESUMED, queue=self)

    @property
    def paused(self) -> bool:
        return self._paused

    @property
    def paused_at(self) -> Optional[m.AwareDatetime]:
        return self._paused_at

    def count(self) -> int:
        return self._store.count()


class _Queues(Generic[T]):
    def __init__(self, queues: tuple[JobQueue[T], ...]) -> None:
        if not queues:
            raise ValueError("a coordinator needs at least one queue")
        first = queues[0]
        if len({(q.coordinator_lock, q.changed) for q in queues}) > 1:
            raise ValueError("every queue must share one lock and one changed event")
        self.queues = queues
        self.lock = first.coordinator_lock
        self._events = first.events

    def lookup(self, name: str) -> Optional[JobQueue[T]]:
        return next((queue for queue in self.queues if queue.name == name), None)

    def find(self, job_id: str, name: str) -> Optional[m.AnyJob[T]]:
        with self.lock:
            queue = self.lookup(name)
            return queue.get(job_id) if queue else None

    def cancel_job(self, job_id: str, name: str) -> bool:
        with self.lock:
            queue = self.lookup(name)
            if not queue or not queue.get(job_id):
                return False
            queue.remove(job_id)
            return True

    def pause_job(self, job_id: str, disable: bool, name: str) -> bool:
        with self.lock:
            queue = self.lookup(name)
            job = queue.get(job_id) if queue else None
            if not queue or not job:
                return False
            job.disable = disable
            queue.update(job)
            return True

    def pause_queue(self, name: str) -> None:
        queue = self.lookup(name)
        if queue:
            with self.lock:
                queue.pause()


class Coordinator(_Queues[T]):
    def __init__(self, *lanes: Lane[T], services: Sequence[Service] = (), context: Any = None) -> None:
        if not lanes:
            raise ValueError("a coordinator needs at least one lane")
        self.lanes = lanes
        self.services = tuple(services)
        self._lane_of: dict[int, Lane[T]] = {}
        for lane in lanes:
            lane.context = context
            for queue in lane.queues:
                if id(queue) in self._lane_of:
                    raise ValueError(f"queue {queue.name!r} is in two lanes")
                self._lane_of[id(queue)] = lane
        super().__init__(tuple(queue for lane in lanes for queue in lane.queues))
        self._wake = self.queues[0].changed
        self._stopped = False
        self._thread = threading.Thread(target=self._watch, name="coordinator", daemon=True)
        self._ask = threading.Event()
        self._found: Optional[asyncio.Future[Optional[JobQueue[T]]]] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._failed: Optional[asyncio.Future[ServiceFailed]] = None
        self._resumed = asyncio.Event()
        self._shut = asyncio.Event()

    def start(self) -> None:
        loop = asyncio.get_running_loop()
        with self.lock:
            self._loop = loop
            self._failed = loop.create_future()
        for lane in self.lanes:
            lane.start()
        for service in self.services:
            service.start(loop, self._failed)
        self._thread.start()
        emit(self._events, m.Event.STARTED)

    async def next(self) -> tuple[JobQueue[T], m.AnyJob[T]]:
        loop = asyncio.get_running_loop()
        while True:
            with self.lock:
                if self._stopped:
                    raise asyncio.CancelledError
                if not self._thread.is_alive():
                    raise RuntimeError("the scan thread is not running")
                self._found = loop.create_future()
            self._ask.set()
            try:
                queue = await self._found
            except asyncio.CancelledError:
                self._wake.set()
                raise
            if not queue:
                continue
            with self.lock:
                if self._stopped:
                    raise asyncio.CancelledError
                popped = queue.pop()
                if popped:
                    lane = self._lane_of[id(queue)]
                    lane.busy = True
                    emit(queue.events, m.Event.SUBMITTED, queue=queue, job=popped)
                    lane.submit(queue, popped)
            if popped:
                return queue, popped

    def _watch(self) -> None:
        while self._running():
            self._ask.wait()
            self._ask.clear()
            if not self._running():
                self._hand_back(self._resolve, None)
                return
            try:
                queue = self._wait_due()
            except UnloadableJob:
                queue = None
            except Exception as exc:
                self._hand_back(self._fail, exc)
                return
            self._hand_back(self._resolve, queue)

    def _running(self) -> bool:
        with self.lock:
            return not self._stopped

    def _hand_back(self, fn: Callable[..., None], value: Any) -> None:
        with self.lock:
            found, loop = self._found, self._loop
        if found and loop:
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(fn, found, value)

    @staticmethod
    def _resolve(found: asyncio.Future[Optional[JobQueue[T]]], queue: Optional[JobQueue[T]]) -> None:
        if not found.done():
            found.set_result(queue)

    @staticmethod
    def _fail(found: asyncio.Future[Optional[JobQueue[T]]], exc: BaseException) -> None:
        if not found.done():
            found.set_exception(exc)

    def _wait_due(self) -> Optional[JobQueue[T]]:
        self._wake.clear()
        wait: Optional[float] = None
        with self.lock:
            if self._stopped:
                return None
            head = _earliest(q for q in self.queues if not self._lane_of[id(q)].busy)
            if head:
                run_at, queue = head
                wait = (run_at - m.utcnow()).total_seconds()
                if wait <= 0:
                    return queue
        self._wake.wait(wait)
        return None

    def resume(self) -> None:
        with self.lock:
            if self._loop:
                self._loop.call_soon_threadsafe(self._resumed.set)
            else:
                self._resumed.set()

    async def run(self) -> None:
        with self.lock:
            failed = self._failed
        if not failed:
            raise RuntimeError("start() first")
        dispatch = asyncio.get_running_loop().create_task(self._dispatch())
        try:
            waiting: set[asyncio.Future[Any]] = {dispatch, failed}
            await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)
        finally:
            dispatch.cancel()
            await asyncio.gather(dispatch, return_exceptions=True)
        if failed.done():
            raise failed.result()
        with self.lock:
            stopped = self._stopped
        if stopped:
            await self._shut.wait()
            return
        dispatch.result()

    async def _dispatch(self) -> None:
        await self._resumed.wait()
        while True:
            await self.next()

    async def stop(self) -> None:
        with self.lock:
            self._stopped = True
        self._resumed.set()
        self._wake.set()
        self._ask.set()
        for lane in self.lanes:
            await lane.stop()
        for service in reversed(self.services):
            await service.stop()
        emit(self._events, m.Event.SHUTDOWN)
        self._shut.set()

    def shutdown(self, timeout: float) -> None:
        with self.lock:
            loop, stopped = self._loop, self._stopped
        if not loop or stopped or loop.is_closed():
            return
        future = asyncio.run_coroutine_threadsafe(self.stop(), loop)
        with contextlib.suppress(TimeoutError):
            future.result(timeout)


class ManualCoordinator(_Queues[T]):
    def __init__(self, *queues: JobQueue[T], context: Any = None) -> None:
        super().__init__(queues)
        self.context = context

    def run_until(self, until: m.AwareDatetime, *, tick: Callable[[m.AwareDatetime], None]) -> None:
        while True:
            with self.lock:
                head = _earliest(self.queues)
            if not head or head[0] > until:
                break
            run_at, queue = head
            tick(max(run_at, m.utcnow()))
            with self.lock:
                job = queue.pop()
                if not job:
                    raise RuntimeError(f"tick did not move the clock to {run_at.isoformat()}")
                emit(queue.events, m.Event.SUBMITTED, queue=queue, job=job)
            job.context = self.context
            try:
                run(job)
            except Exception as exc:
                _log.exception("job %s on queue %r failed", job.id, queue.name)
                with self.lock:
                    emit(queue.events, m.Event.ERROR, queue=queue, job=job, error=exc)
            else:
                with self.lock:
                    emit(queue.events, m.Event.EXECUTED, queue=queue, job=job)
        tick(until)


class Lane(abc.ABC, Generic[T]):
    def __init__(self, *queues: JobQueue[T]) -> None:
        if not queues:
            raise ValueError("a lane needs at least one queue")
        self.queues = queues
        self.busy = False
        self.context: Any = None
        self._lock = queues[0].coordinator_lock
        self._wake = queues[0].changed

    @abc.abstractmethod
    def start(self) -> None: ...

    @abc.abstractmethod
    def submit(self, queue: JobQueue[T], job: m.AnyJob[T]) -> None: ...

    @abc.abstractmethod
    async def stop(self) -> None: ...

    def _done(self, queue: JobQueue[T], job: m.AnyJob[T], exc: Optional[Exception]) -> None:
        if exc:
            _log.error("job %s on queue %r failed", job.id, queue.name, exc_info=exc)
            self._report(queue, m.Event.ERROR, job=job, error=exc)
        else:
            self._report(queue, m.Event.EXECUTED, job=job)
        with self._lock:
            self.busy = False
        self._wake.set()

    def _report(self, queue: JobQueue[T], event: m.Event, **info: Any) -> None:
        try:
            with self._lock:
                emit(queue.events, event, queue=queue, **info)
        except Exception:
            _log.exception("%s listener raised for job %s on queue %r", event, info["job"].id, queue.name)


class ThreadLane(Lane[T]):
    def __init__(self, *queues: JobQueue[T]) -> None:
        super().__init__(*queues)
        self._handed: Optional[tuple[JobQueue[T], m.AnyJob[T]]] = None
        self._ready = threading.Event()
        self._stopping = threading.Event()
        self._thread = threading.Thread(target=self._serve, name="lane", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def submit(self, queue: JobQueue[T], job: m.AnyJob[T]) -> None:
        with self._lock:
            self._handed = (queue, job)
        self._ready.set()

    async def stop(self) -> None:
        self._stopping.set()
        self._ready.set()
        await _join(self._thread)

    def _serve(self) -> None:
        while True:
            self._ready.wait()
            self._ready.clear()
            with self._lock:
                handed, self._handed = self._handed, None
            if handed:
                self._call(*handed)
            if self._stopping.is_set():
                return

    def _call(self, queue: JobQueue[T], job: m.AnyJob[T]) -> None:
        job.context = self.context
        exc: Optional[Exception] = None
        try:
            run(job)
        except Exception as e:
            exc = e
        self._done(queue, job, exc)


class AsyncLane(Lane[T]):
    def __init__(self, *queues: JobQueue[T], timeout: Optional[float] = None) -> None:
        super().__init__(*queues)
        self._timeout = timeout
        self._task: Optional[asyncio.Task[None]] = None

    def start(self) -> None:
        return None

    def submit(self, queue: JobQueue[T], job: m.AnyJob[T]) -> None:
        self._task = asyncio.get_running_loop().create_task(self._execute(queue, job))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self._task = None

    async def _execute(self, queue: JobQueue[T], job: m.AnyJob[T]) -> None:
        job.context = self.context
        exc: Optional[Exception] = None
        try:
            async with asyncio.timeout(self._timeout):
                result = run(job)
                if inspect.isawaitable(result):
                    await result
        except Exception as e:
            exc = e
        self._done(queue, job, exc)


class ServiceFailed(RuntimeError):
    def __init__(self, service: Service, cause: Optional[BaseException]) -> None:
        super().__init__(f"service {service.name!r} failed {GIVE_UP_AFTER} times in a row")
        self.service = service
        self.__cause__ = cause


class Service(abc.ABC):
    def __init__(
        self, fn: Callable[..., Any], *, name: Optional[str] = None, restart_after: float = 1.0, stop_timeout: float = 10.0
    ) -> None:
        self._fn = fn
        self.name: str = name if name else str(getattr(fn, "__name__", repr(fn)))
        self.restart_after = restart_after or math.inf
        self._stop_timeout = stop_timeout
        self.restarts = 0
        self._fast = 0
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._failed: Optional[asyncio.Future[ServiceFailed]] = None

    def start(self, loop: asyncio.AbstractEventLoop, failed: asyncio.Future[ServiceFailed]) -> None:
        self._loop = loop
        self._failed = failed

    @abc.abstractmethod
    async def stop(self) -> None: ...

    def _exited(self, started: float, stopped: bool, exc: Optional[BaseException]) -> bool:
        if stopped:
            return False
        if not exc:
            _log.error("service %s exited on its own", self.name)
        else:
            _log.error("service %s crashed", self.name, exc_info=exc)
        if self.restart_after == math.inf:
            _log.error("service %s will not be restarted", self.name)
            return False
        self._fast = self._fast + 1 if time.monotonic() - started < self.restart_after else 0
        if self._fast >= GIVE_UP_AFTER:
            _log.error("service %s failed %d times in a row, giving up", self.name, self._fast)
            assert self._loop and self._failed
            self._loop.call_soon_threadsafe(self._report, ServiceFailed(self, exc))
            return False
        self.restarts += 1
        _log.warning("service %s restarting in %.1fs (restart %d)", self.name, self.restart_after, self.restarts)
        return True

    def _report(self, failure: ServiceFailed) -> None:
        if self._failed and not self._failed.done():
            self._failed.set_result(failure)


class AsyncService(Service):
    def __init__(self, fn: Callable[[asyncio.Event], Awaitable[None]], **kw: Any) -> None:
        super().__init__(fn, **kw)
        self._stop = asyncio.Event()
        self._task: Optional[asyncio.Task[None]] = None

    def start(self, loop: asyncio.AbstractEventLoop, failed: asyncio.Future[ServiceFailed]) -> None:
        super().start(loop, failed)
        self._stop = asyncio.Event()
        self._task = loop.create_task(self._supervise(), name=f"service-{self.name}")

    async def stop(self) -> None:
        self._stop.set()
        if not self._task:
            return
        try:
            await asyncio.wait_for(asyncio.shield(self._task), self._stop_timeout)
        except TimeoutError:
            _log.warning("service %s ignored stop for %.1fs, cancelling", self.name, self._stop_timeout)
            self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await self._task
        self._task = None

    async def _supervise(self) -> None:
        while True:
            started = time.monotonic()
            exc: Optional[BaseException] = None
            try:
                await self._fn(self._stop)
            except Exception as e:
                exc = e
            if not self._exited(started, self._stop.is_set(), exc):
                return
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), self.restart_after)
                return


class ThreadService(Service):
    def __init__(self, fn: Callable[[threading.Event], None], **kw: Any) -> None:
        super().__init__(fn, **kw)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._supervise, name=f"service-{self.name}", daemon=True)

    def start(self, loop: asyncio.AbstractEventLoop, failed: asyncio.Future[ServiceFailed]) -> None:
        super().start(loop, failed)
        self._thread.start()

    async def stop(self) -> None:
        self._stop.set()
        await _join(self._thread, self._stop_timeout)
        if self._thread.is_alive():
            _log.warning("service %s ignored stop for %.1fs; thread abandoned", self.name, self._stop_timeout)

    def _supervise(self) -> None:
        while True:
            started = time.monotonic()
            exc: Optional[BaseException] = None
            try:
                self._fn(self._stop)
            except Exception as e:
                exc = e
            if not self._exited(started, self._stop.is_set(), exc):
                return
            if self._stop.wait(min(self.restart_after, threading.TIMEOUT_MAX)):
                return


def on(events: m.Events, event: m.Event, fn: Callable[..., None]) -> None:
    events.setdefault(event, []).append(fn)


def emit(events: m.Events, event: m.Event, **info: Any) -> None:
    for fn in events.get(event, ()):
        fn(**info)


def run(job: m.AnyJob[Any]) -> Any:
    params = inspect.signature(job.fn).parameters
    offered = {"ctx": job.context, "payload": job.payload}
    return job.fn(**{name: value for name, value in offered.items() if name in params})


def evictable(job: m.AnyJob[Any]) -> bool:
    return not isinstance(job, m.CronJob)


def next_instance(job: m.AnyJob[T], after: Optional[m.AwareDatetime] = None) -> Optional[m.AnyJob[T]]:
    if not isinstance(job, m.CronJob):
        return None
    start = max(job.run_at, after or m.utcnow()).astimezone(job.run_at.tzinfo)
    run_at = m.aware(next(CronSim(job.cron, start)), "run_at")
    return replace(job, run_at=run_at, id=job.id_template.format(uuid=uuid.uuid4()))


def schedule(
    cron: str, tz: tzinfo, fn: Callable[..., Any], *, id_template: str = "{uuid}", payload: Optional[T] = None, name: Optional[str] = None
) -> m.CronJob[T]:
    first = m.aware(next(CronSim(cron, m.utcnow().astimezone(tz))), "run_at")
    return m.CronJob(first, fn, payload=payload, id=id_template.format(uuid=uuid.uuid4()), name=name, cron=cron, id_template=id_template)


def _earliest(queues: Iterable[JobQueue[T]]) -> Optional[tuple[m.AwareDatetime, JobQueue[T]]]:
    heads = [(job.run_at, queue) for queue in queues if not queue.paused and (job := queue.next_job())]
    return min(heads, key=lambda head: head[0]) if heads else None


async def _join(thread: threading.Thread, timeout: Optional[float] = None) -> None:
    deadline = None if timeout is None else time.monotonic() + timeout
    while thread.is_alive() and (deadline is None or time.monotonic() < deadline):
        await asyncio.sleep(0.01)
