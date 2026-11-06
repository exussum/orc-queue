from datetime import timedelta

from conftest import at, now

from orc_queue import Event, Job, ManualCoordinator, on, schedule


def test_runs_due_jobs_in_slot_order_across_queues_each_at_its_slot(two_queues, freezegun_clock):
    a, b = two_queues
    seen = []

    def record(payload):
        seen.append((payload, now().replace(microsecond=0)))

    with a.coordinator_lock:
        a.add(Job(at(14), record, "a-14"))
        b.add(Job(at(13), record, "b-13"))
        a.add(Job(at(16), record, "a-16"))
    ManualCoordinator(a, b).run_until(at(15), tick=freezegun_clock.move_to)

    assert seen == [("b-13", at(13)), ("a-14", at(14))]
    assert now().replace(microsecond=0) == at(15)
    with a.coordinator_lock:
        assert [job.payload for job in a.jobs()] == ["a-16"]


def test_skips_paused_queues_respawns_crons_and_reports_failures(two_queues, events, freezegun_clock):
    a, b = two_queues
    ran, errors = [], []
    on(events, Event.ERROR, lambda queue, job, error: errors.append(job.payload))

    def boom(payload):
        raise ValueError(payload)

    with a.coordinator_lock:
        a.add(Job(at(13), boom, "fails"))
        a.add(schedule("0 * * * *", at(12).tzinfo, lambda ctx: ran.append(ctx), id_template="hourly"))
        b.add(Job(at(13), lambda: ran.append("held"), "held"))
        b.pause()
    ManualCoordinator(a, b, context="ctx").run_until(at(14) + timedelta(minutes=30), tick=freezegun_clock.move_to)

    assert errors == ["fails"]
    assert ran == ["ctx", "ctx"]
    with a.coordinator_lock:
        assert [job.id for job in a.jobs()] == ["hourly"] and a.jobs()[0].run_at == at(15)
        assert [job.payload for job in b.jobs()] == ["held"]
