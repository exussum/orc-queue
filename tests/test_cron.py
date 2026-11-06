import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest
from conftest import at, noop, now
from cronsim import CronSimError

from orc_queue import CronJob, Job, next_instance, schedule

UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
NY = ZoneInfo("America/New_York")


def test_schedule_picks_next_slot_and_renders_id_template():
    before = now()
    job = schedule("*/15 * * * *", timezone.utc, noop, id_template="report-{uuid}")
    assert job.run_at > before and job.run_at.minute % 15 == 0 and job.run_at.second == 0
    assert job.id.startswith("report-") and UUID.fullmatch(job.id[len("report-") :])
    with pytest.raises(CronSimError):
        CronJob(at(1), noop, cron="not a cron")


def test_next_instance_clones_with_new_id_and_skips_missed_slots():
    job = CronJob(datetime(2026, 11, 2, 9, tzinfo=NY), noop, cron="0 9 * * *", id_template="daily-{uuid}", payload={"k": 1})
    nxt = next_instance(job, after=job.run_at)
    assert nxt.id != job.id and nxt.id.startswith("daily-")
    assert nxt.run_at == job.run_at + timedelta(days=1) and nxt.run_at.tzinfo == NY
    assert (nxt.cron, nxt.id_template, nxt.payload) == (job.cron, job.id_template, job.payload)
    assert next_instance(job, after=job.run_at + timedelta(days=4, hours=12)).run_at == job.run_at + timedelta(days=5)


def test_next_instance_across_dst_end():
    start = datetime(2026, 11, 1, 1, 30, tzinfo=NY, fold=0)
    nxt = next_instance(CronJob(start, noop, cron="30 1 * * *"), after=start)
    assert nxt.run_at.astimezone(timezone.utc) > start.astimezone(timezone.utc)
    assert (nxt.run_at.hour, nxt.run_at.minute) == (1, 30)


def test_pop_respawns_the_next_occurrence(queue, freezegun_clock):
    freezegun_clock.move_to(at(1))
    with queue.coordinator_lock:
        queue.add(Job(at(5), noop, id="plain"))
    job = CronJob(at(1), noop, cron="0 */2 * * *", id_template="tick-{uuid}", payload="p")
    with queue.coordinator_lock:
        queue.add(job)
    with queue.coordinator_lock:
        popped = queue.pop()
    assert popped.id == job.id
    with queue.coordinator_lock:
        clone = queue.next_job()
    assert isinstance(clone, CronJob) and clone.id != job.id and clone.run_at == at(2) and clone.payload == "p"
    freezegun_clock.move_to(at(7) + timedelta(minutes=10))
    with queue.coordinator_lock:
        popped2 = queue.pop()
        assert popped2.id == clone.id and queue.next_job().run_at == at(5)
    with queue.coordinator_lock:
        assert (queue.jobs())[-1].run_at == at(8)


def test_remove_and_add_do_not_respawn(queue, freezegun_clock):
    freezegun_clock.move_to(at(1))
    with queue.coordinator_lock:
        queue.add(CronJob(at(1), noop, cron="* * * * *", id="r"))
    with queue.coordinator_lock:
        queue.remove("r")
    with queue.coordinator_lock:
        assert queue.count() == 0
    with queue.coordinator_lock:
        queue.add(CronJob(at(5), noop, cron="0 * * * *", id="c"))
    with queue.coordinator_lock:
        queue.add(CronJob(at(1), noop, cron="30 * * * *", id="c"))
    with queue.coordinator_lock:
        popped = queue.pop()
        assert popped.cron == "30 * * * *" and queue.next_job().run_at == at(1) + timedelta(minutes=30)
    with queue.coordinator_lock:
        assert queue.count() == 1


def test_disabled_cron_is_deleted_when_skipped(queue):
    job = CronJob(at(1), noop, cron="0 * * * *", id="h")
    with queue.coordinator_lock:
        queue.add(job)
    job.disable = True
    with queue.coordinator_lock:
        queue.update(job)
    with queue.coordinator_lock:
        assert queue.next_job() is None
    with queue.coordinator_lock:
        assert queue.count() == 0


def test_queue_pause_catches_up_once_on_resume(queue, freezegun_clock):
    freezegun_clock.move_to(at(0))
    job = CronJob(at(1), noop, cron="0 * * * *")
    with queue.coordinator_lock:
        queue.add(job)
    with queue.coordinator_lock:
        queue.pause()
    freezegun_clock.move_to(at(1) + timedelta(minutes=30))
    with queue.coordinator_lock:
        queue.resume()
    with queue.coordinator_lock:
        popped = queue.pop()
        assert popped.id == job.id and popped.run_at == at(1) and queue.next_job().run_at == at(2)
