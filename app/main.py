from uuid import uuid4
import json

from fastapi import FastAPI

from app.models import JobRequest
from app.redis_client import redis_client

app = FastAPI(title="Cerberus")

@app.get("/")
def root():
    return {"message": "Cerberus is running"}

@app.post("/jobs")
def create_job(job: JobRequest):
    # 1. Generate a unique ID for the job
    job_id = str(uuid4())

    # 2. Convert the Python dictionary into a JSON string
    payload = json.dumps(job.payload)

    # 3. Add the job to the Redis Stream
    stream_id = redis_client.xadd(
        "cerberus:jobs",
        {
            "job_id": job_id,
            "type": job.type,
            "payload": payload,
            "attempt": "0"
        }
    )

    # 4. Return information about the submitted job
    return {
        "job_id": job_id,
        "stream_id": stream_id,
        "type": job.type,
        "payload": job.payload,
        "status": "queued"
    }