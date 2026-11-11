
import json
from fastapi.testclient import TestClient

from app.main import app
from app import main

client = TestClient(app)


def test_create_job_success(monkeypatch):
    db_calls = []
    redis_calls = []

    def mock_create_job(job_id, job_type, payload):
        db_calls.append((job_id, job_type, payload))

    def mock_redis_xadd(stream, fields):
        redis_calls.append((stream, fields))
        return "123-0"

    monkeypatch.setattr(main, "create_job", mock_create_job)
    monkeypatch.setattr(main.redis_client, "xadd", mock_redis_xadd)

    response = client.post(
        "/jobs",
        json={
            "type": "normal",
            "payload": {"message": "Hello Cerberus"}
        }
    )

    assert response.status_code == 200

    data = response.json()

    assert data["type"] == "normal"
    assert data["payload"] == {"message": "Hello Cerberus"}
    assert data["status"] == "queued"
    assert data["stream_id"] == "123-0"

    assert len(db_calls) == 1
    assert db_calls[0][1] == "normal"

    assert len(redis_calls) == 1
    assert redis_calls[0][0] == "cerberus:jobs"
    assert redis_calls[0][1]["type"] == "normal"
    assert json.loads(redis_calls[0][1]["payload"]) == {
        "message": "Hello Cerberus"
    }


def test_create_job_invalid_request():
    response = client.post(
        "/jobs",
        json={"type": "normal"}
    )

    assert response.status_code == 422
