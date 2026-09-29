import time
import json

from app.redis_client import redis_client

RETRY_ZSET = "cerberus:retries"
STREAM_NAME = "cerberus:jobs"

def get_ready_retries():
    current_time = time.time()

    ready_jobs = redis_client.zrangebyscore(
        RETRY_ZSET,
        0,
        current_time
    )

    return ready_jobs

def process_ready_retry(retry_job):
    job = json.loads(retry_job)

    redis_client.xadd(
        STREAM_NAME,
        {
            "job_id": job["job_id"],
            "type": job["type"],
            "payload": job["payload"],
            "attempt": job["attempt"]
        }
    )

    redis_client.zrem(RETRY_ZSET, retry_job)

    print("Retry moved back to job stream:", job["job_id"])

def main():
    print("Cerberus retry scheduler started...")

    while True:
        ready_jobs = get_ready_retries()

        print("Ready retries:", len(ready_jobs))

        for retry_job in ready_jobs:
            process_ready_retry(retry_job)

        time.sleep(1)

if __name__ == "__main__":
    main()