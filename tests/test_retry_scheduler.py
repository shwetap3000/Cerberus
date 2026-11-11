
import json
from app import retry_scheduler


def test_get_ready_retries(monkeypatch):
    expected_jobs = [
        '{"job_id": "job-1"}',
        '{"job_id": "job-2"}'
    ]

    monkeypatch.setattr(
        retry_scheduler.time,
        "time",
        lambda: 1000
    )

    def mock_zrangebyscore(key, minimum, maximum):
        assert key == "cerberus:retries"
        assert minimum == 0
        assert maximum == 1000
        return expected_jobs

    monkeypatch.setattr(
        retry_scheduler.redis_client,
        "zrangebyscore",
        mock_zrangebyscore
    )

    result = retry_scheduler.get_ready_retries()

    assert result == expected_jobs


def test_process_ready_retry(monkeypatch):
    retry_job = {
        "job_id": "test-job-123",
        "type": "normal",
        "payload": json.dumps({"message": "Retry test"}),
        "attempt": "1"
    }

    retry_job_json = json.dumps(retry_job)

    xadd_calls = []
    zrem_calls = []

    monkeypatch.setattr(
        retry_scheduler.redis_client,
        "xadd",
        lambda stream, fields: xadd_calls.append((stream, fields))
    )

    monkeypatch.setattr(
        retry_scheduler.redis_client,
        "zrem",
        lambda key, value: zrem_calls.append((key, value))
    )

    retry_scheduler.process_ready_retry(retry_job_json)

    assert len(xadd_calls) == 1

    stream, fields = xadd_calls[0]

    assert stream == "cerberus:jobs"
    assert fields["job_id"] == "test-job-123"
    assert fields["type"] == "normal"
    assert fields["attempt"] == "1"

    assert json.loads(fields["payload"]) == {
        "message": "Retry test"
    }

    assert len(zrem_calls) == 1
    assert zrem_calls[0] == (
        "cerberus:retries",
        retry_job_json
    )
