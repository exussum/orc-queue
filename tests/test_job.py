from datetime import timedelta

from conftest import at, noop

from orc_queue import Event, Job, next_instance, on


def test_job_defaults_and_hooks():
    a, b = Job(at(1), None, noop), Job(at(1), None, noop)
    assert a.id != b.id and len(a.id) == len(b.id) == 36
    assert next_instance(a) is None


def test_a_job_inside_its_deadline_runs(queue):
    with queue.coordinator_lock:
        queue.add(Job(at(11), at(13), noop, id="fresh"))
    with queue.coordinator_lock:
        assert queue.pop().id == "fresh"


def test_a_job_past_its_deadline_is_dropped(queue, events):
    expired = []
    on(events, Event.EXPIRED, lambda **info: expired.append(info["job"].id))
    with queue.coordinator_lock:
        queue.add(Job(at(11), at(11) + timedelta(minutes=30), noop, id="stale"))
    with queue.coordinator_lock:
        assert queue.pop() is None
    with queue.coordinator_lock:
        assert queue.count() == 0
    assert expired == ["stale"]


def test_an_expired_head_does_not_hide_the_next_job(queue):
    with queue.coordinator_lock:
        queue.add(Job(at(10), at(10) + timedelta(minutes=30), noop, id="stale"))
    with queue.coordinator_lock:
        queue.add(Job(at(11), None, noop, id="live"))
    with queue.coordinator_lock:
        assert queue.pop().id == "live"
