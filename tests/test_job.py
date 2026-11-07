from conftest import at, noop

from orc_queue import Job, job, next_instance


def test_job_defaults_and_hooks():
    a, b = Job(at(1), noop), Job(at(1), noop)
    assert a.id != b.id and len(a.id) == 36
    assert next_instance(a) is None


def test_job_optional_id():
    assert job(at(1), noop, id="x").id == "x"
    assert len(job(at(1), noop).id) == 36
