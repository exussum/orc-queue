# orc-queue

A job queue for running tasks in order, while managing background threads.

## Quick start

```python
import asyncio
import threading
from datetime import timedelta

from orc_queue import Coordinator, Events, Job, JobQueue, MemoryStore, ThreadLane, utcnow

shared_lock = threading.RLock()
changed = threading.Event()
listeners = Events()

greetings = JobQueue(coordinator_lock=shared_lock, changed=changed, store=MemoryStore(), cap=0, events=listeners, name="greetings")
with greetings.coordinator_lock:
    greetings.add(Job(run_at=utcnow() + timedelta(seconds=2), fn=lambda payload: print("ran", payload), payload="hello", id="greet"))

coordinator = Coordinator(ThreadLane(greetings))


async def main() -> None:
    coordinator.start()
    coordinator.resume()
    task = asyncio.create_task(coordinator.run())
    await asyncio.sleep(3)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await coordinator.stop()


asyncio.run(main())
```

Two seconds after start this prints `ran hello`.

Every queue under one coordinator is built with the same `RLock` and the same
`Event`. Hold the lock, with `with queue.coordinator_lock:`, around every
queue call; a call without it raises `RuntimeError`. Holding it across several
calls makes them atomic.

## Jobs

### `Job(run_at, fn, payload=None, id=..., disable=False, name=None)`

A one-off job.

- `run_at`: when to run it. Must be timezone-aware.
- `fn`: the function to call. It receives `payload` and `ctx` as keyword
  arguments, but only the ones it names, so `lambda: ...`,
  `def fn(payload)` and `def fn(payload, *, ctx)` all work. `ctx` is the
  coordinator's `context`. `fn` may be a coroutine function when it runs on
  an `AsyncLane`.
- `payload`: any value handed to `fn`.
- `id`: defaults to a random uuid. Adding a job with an id already in the
  queue replaces that job.
- `disable`: `True` skips this job; see [Pausing](#pausing).
- `name`: a label for your own use.

### `job(run_at, fn, payload=None, *, id=None, name=None) -> Job`

A one-off job, as `Job` builds it, where `id=None` means a random uuid. Use
it when the caller may or may not have an id to give.

### `schedule(cron, tz, fn, *, id_template="{uuid}", payload=None, name=None) -> CronJob`

A repeating job on a cron expression, evaluated in timezone `tz`. Its first run
is the next matching time. Each time it runs, the next occurrence is queued
automatically; occurrences missed while the program was down or the queue was
paused are skipped, not replayed. `id_template` sets each occurrence's id;
`{uuid}` is replaced with a fresh uuid.

### `utcnow() -> datetime` and `aware(dt) -> datetime`

`utcnow()` is the current time in UTC, timezone-aware. `aware(dt)` returns
`dt` unchanged, or raises `ValueError` if it has no timezone.

## Queues

### `JobQueue(coordinator_lock, changed, store, cap, events, name=None, on_full=OnFull.REJECT)`

- `coordinator_lock`, `changed`: the lock and event shared by every queue under
  one coordinator.
- `store`: where the jobs are kept; see [Stores](#stores).
- `cap`: the most jobs the queue holds. `0` means unlimited.
- `events`: an `Events()` holding your listeners; see [Events](#events).
- `name`: how the coordinator finds the queue.
- `on_full`: what `add` does when the queue is full. `OnFull.REJECT` refuses
  the new job; `OnFull.EVICT` drops the earliest one-off job to make room.
  Repeating jobs are never evicted.

Methods, all called with the lock held:

| Method                          | Does                                                                                                               |
|---------------------------------|--------------------------------------------------------------------------------------------------------------------|
| `add(job) -> bool`              | Adds the job, or replaces the one with its id. `False` if the queue is full and nothing could be evicted.          |
| `update(job) -> None`           | Saves changes to a job already in the queue, such as a new `run_at` or `disable`. `KeyError` if the id is unknown. |
| `get(job_id) -> Job \| None`    | The job with that id, or `None`.                                                                                   |
| `remove(job_id) -> Job`         | Deletes and returns the job. `KeyError` if the id is unknown.                                                      |
| `next_job() -> Job \| None`     | The next job that will run, without taking it.                                                                     |
| `jobs() -> list[Job]`           | Every job, soonest first, disabled ones last.                                                                      |
| `count() -> int`                | How many jobs the queue holds.                                                                                     |
| `reset() -> None`               | Deletes every one-off job; repeating jobs stay. A paused queue stays paused.                                       |
| `pause() -> None`               | Holds the queue: none of its jobs run until `resume()`.                                                            |
| `resume() -> None`              | Releases the queue.                                                                                                |
| `paused -> bool`                | Whether the queue is paused.                                                                                       |
| `paused_at -> datetime \| None` | When it was paused, or `None`.                                                                                     |

### Stores

- `MemoryStore()`: jobs live in memory and are lost on exit.
- `SqliteStore(path, name="default", discard_after=timedelta(minutes=5))`:
  jobs survive restarts in the sqlite file at `path`. Several queues can share
  one file under different `name`s. Jobs are pickled, so `fn` and `payload`
  must be picklable (a module-level function, not a lambda). On open, one-off
  jobs more than `discard_after` overdue are dropped, as are jobs that can no
  longer be loaded (for example, because `fn` was renamed). A job that fails to
  load after that raises `UnloadableJob`.

### Pausing

- Disable a job: set `job.disable = True` and call `queue.update(job)`. A
  one-off job stays in the queue, and can be re-enabled, until its time
  passes; then it is deleted without running. A repeating job is never
  deleted: each time it comes due it moves to its next slot without running,
  still disabled, until it is re-enabled.
- Hold a whole queue: `queue.pause()`. Nothing in it runs until `resume()`;
  then overdue jobs run in order, and a repeating job runs once to catch up.

## Coordinator

### `Coordinator(*lanes, services=(), context=None)`

Runs jobs from every queue in `lanes` as they come due, and supervises
`services`. `context` is passed to every job's `fn` as `ctx`. A queue may
belong to only one lane.

| Method                                     | Does                                                                                                                                |
|--------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| `start() -> None`                          | Starts the lanes and services. Call from inside the running event loop, before `run()`.                                             |
| `resume() -> None`                         | Lets jobs run. A coordinator starts paused, so setup can finish first.                                                              |
| `await run() -> None`                      | Runs jobs until cancelled. Raises `ServiceFailed` if a service gives up, or a store error.                                          |
| `await stop() -> None`                     | Stops taking jobs, waits for the running ones to finish, then stops the services; `run()` then returns.                             |
| `shutdown(timeout) -> None`                | Calls `stop()` from any other thread and waits up to `timeout` seconds. Does nothing if it was never started or is already stopped. |
| `lookup(name) -> JobQueue \| None`         | The queue with that name, or `None`.                                                                                                |
| `find(job_id, name) -> Job \| None`        | The job with that id in the named queue, or `None`.                                                                                 |
| `cancel_job(job_id, name) -> bool`         | Deletes the job from the named queue. `False` if it isn't there.                                                                    |
| `pause_job(job_id, disable, name) -> bool` | Disables or re-enables the job. `False` if it isn't there.                                                                          |
| `pause_queue(name) -> None`                | Pauses the named queue. Unknown names are ignored.                                                                                  |

The coordinator's methods take the lock themselves; don't hold it around them.

### Lanes

A lane runs the jobs of its queues one at a time, soonest first. Separate lanes
run in parallel, so put jobs that must not overlap in the same lane.

- `ThreadLane(*queues)`: runs jobs on a thread of its own.
- `AsyncLane(*queues, timeout=None)`: runs jobs on the event loop, awaiting
  coroutine functions. A job running longer than `timeout` seconds is
  cancelled.

### Manual coordinator

`ManualCoordinator(*queues, context=None)` runs the same queues without
threads, lanes or an event loop, for tests that need to step through time. It
has the coordinator's `lookup`, `find`, `cancel_job`, `pause_job` and
`pause_queue`, plus:

- `run_until(until, *, tick)`: runs every job due by `until`, one at a time on
  the calling thread, earliest first across all its queues. Before each job it
  calls `tick` with the job's time, and at the end with `until`; pass a frozen
  clock's `move_to` (freezegun) so each job runs at its own time. Paused queues
  are skipped, repeating jobs come back at their next time, and the same
  `SUBMITTED`, `EXECUTED` and `ERROR` events fire.

### Services

`ThreadService(fn, *, name=None, restart_after=1.0, stop_timeout=10.0)` and
`AsyncService(...)`, with the same arguments, keep a long-running function
alive: a listener, a connection loop. `fn` takes a stop event and should
return once it is set; `ThreadService` passes a `threading.Event` and runs `fn`
on its own thread, `AsyncService` passes an `asyncio.Event` and runs `fn` as a
task.

- If `fn` returns or raises, it is restarted after `restart_after` seconds.
  `0` means never restart.
- If it fails three times in a row, each within `restart_after` of starting,
  the service gives up and `coordinator.run()` raises `ServiceFailed`.
- On `stop()`, the stop event is set and `fn` gets `stop_timeout` seconds to
  return.

## Events

`Events()` holds listeners. Subscribe with `on(events, Event.ADDED, fn)`; `fn`
is called with keyword arguments.

| Event                        | Fires when                           | Arguments               |
|------------------------------|--------------------------------------|-------------------------|
| `STARTED`                    | the coordinator starts               | none                    |
| `SHUTDOWN`                   | the coordinator has stopped          | none                    |
| `ADDED`                      | a job is added                       | `queue`, `job`          |
| `MODIFIED`                   | a job is replaced or updated         | `queue`, `job`          |
| `REMOVED`                    | a job is removed, evicted or skipped | `queue`, `job`          |
| `SUBMITTED`                  | a job is handed to its lane to run   | `queue`, `job`          |
| `EXECUTED`                   | a job finished                       | `queue`, `job`          |
| `ERROR`                      | a job raised                         | `queue`, `job`, `error` |
| `PAUSED`, `RESUMED`, `RESET` | a queue is paused, resumed or reset  | `queue`                 |

Listeners run while the lock is held: keep them short, and don't wait on
another thread that uses a queue.
