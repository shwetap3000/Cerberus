
import json
import sys
import pytest

sys.argv = ["worker.py", "test-worker"]

from app import worker


@pytest.fixture
def job():
    return {
        "job_id": "test-job-123",
        "type": "normal",
        "payload": json.dumps({"message": "Hello"}),
        "attempt": "0"
    }


def test_normal_job_completes(monkeypatch, job):
    status_calls = []
    ack_calls = []
    processed_calls = []

    monkeypatch.setattr(
        worker,
        "update_job_status",
        lambda *args: status_calls.append(args)
    )

    monkeypatch.setattr(
        worker,
        "process_job",
        lambda job: None
    )

    monkeypatch.setattr(
        worker,
        "check_idempotency",
        lambda job_id: False
    )

    monkeypatch.setattr(
        worker,
        "mark_job_processed",
        lambda job_id: processed_calls.append(job_id)
    )

    monkeypatch.setattr(
        worker.redis_client,
        "xack",
        lambda *args: ack_calls.append(args)
    )

    worker.handle_job("1-0", job)

    assert status_calls[0][1] == "processing"
    assert status_calls[1][1] == "completed"
    assert processed_calls == ["test-job-123"]
    assert len(ack_calls) == 1


def test_already_processed_job_is_skipped(monkeypatch, job):
    ack_calls = []

    monkeypatch.setattr(
        worker,
        "check_idempotency",
        lambda job_id: True
    )

    monkeypatch.setattr(
        worker.redis_client,
        "xack",
        lambda *args: ack_calls.append(args)
    )

    worker.handle_job("1-0", job)

    assert len(ack_calls) == 1


def test_failed_job_schedules_retry(monkeypatch, job):
    retry_calls = []
    ack_calls = []

    monkeypatch.setattr(
        worker,
        "check_idempotency",
        lambda job_id: False
    )

    monkeypatch.setattr(
        worker,
        "update_job_status",
        lambda *args: None
    )

    def fail_job(job):
        raise Exception("Test failure")

    monkeypatch.setattr(
        worker,
        "process_job",
        fail_job
    )

    monkeypatch.setattr(
        worker.redis_client,
        "xack",
        lambda *args: ack_calls.append(args)
    )

    monkeypatch.setattr(
        worker.redis_client,
        "zadd",
        lambda key, mapping: retry_calls.append((key, mapping))
    )

    monkeypatch.setattr(
        worker.random,
        "uniform",
        lambda a, b: 0
    )

    worker.handle_job("1-0", job)

    assert len(ack_calls) == 1
    assert len(retry_calls) == 1

    retry_job = json.loads(
        next(iter(retry_calls[0][1]))
    )

    assert retry_job["attempt"] == "1"
    assert retry_job["job_id"] == "test-job-123"


def test_max_attempts_moves_job_to_dlq(monkeypatch, job):
    dlq_calls = []
    ack_calls = []

    job["attempt"] = "4"

    monkeypatch.setattr(
        worker,
        "check_idempotency",
        lambda job_id: False
    )

    monkeypatch.setattr(
        worker,
        "update_job_status",
        lambda *args: None
    )

    monkeypatch.setattr(
        worker,
        "process_job",
        lambda job: (_ for _ in ()).throw(
            Exception("Test failure")
        )
    )

    monkeypatch.setattr(
        worker.redis_client,
        "xadd",
        lambda stream, fields: dlq_calls.append((stream, fields))
    )

    monkeypatch.setattr(
        worker.redis_client,
        "xack",
        lambda *args: ack_calls.append(args)
    )

    worker.handle_job("1-0", job)

    assert len(dlq_calls) == 1
    assert dlq_calls[0][0] == worker.DLQ_STREAM_NAME
    assert dlq_calls[0][1]["reason"] == "maximum_attempts_exceeded"
    assert len(ack_calls) == 1


def test_reclaim_pending_jobs(monkeypatch):
    expected_result = ("0-0", [("1-0", {"job_id": "test-job"})])

    monkeypatch.setattr(
        worker.redis_client,
        "xautoclaim",
        lambda **kwargs: expected_result
    )

    result = worker.reclaim_pending_jobs()

    assert result == expected_result
