from fastapi import FastAPI
from uuid import uuid4
import json

from app.models import JobRequest
from app.redis_client import redis_client
from app.postgres_client import create_job

app = FastAPI()

@app.post("/jobs")
def create_new_job(job: JobRequest):

    # Generate a unique ID for the job
    job_id = uuid4()

    # Store the job in PostgreSQL
    create_job(
        job_id,
        job.type,
        job.payload
    )

    # Convert payload to JSON string for Redis
    payload = json.dumps(job.payload)

    # Add job to Redis Stream
    stream_id = redis_client.xadd(
        "cerberus:jobs",
        {
            "job_id": str(job_id),
            "type": job.type,
            "payload": payload,
            "attempt": "0"
        }
    )

    return {
        "job_id": str(job_id),
        "stream_id": stream_id,
        "type": job.type,
        "payload": job.payload,
        "status": "queued"
    }