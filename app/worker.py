import json
import sys
import time
import random

from app.redis_client import redis_client
from app.postgres_client import update_job_status
from redis.exceptions import ResponseError

from app.metrics import (
    jobs_completed,
    jobs_failed,
    jobs_retried,
    jobs_dead_lettered
)

STREAM_NAME = "cerberus:jobs"
GROUP_NAME = "cerberus-workers"

RETRY_ZSET = "cerberus:retries"
DLQ_STREAM_NAME = "cerberus:dlq"
IDEMPOTENCY_PREFIX = "cerberus:processed:"

BASE_RETRY_DELAY = 1
MAX_ATTEMPTS = 5

# Maximum random variation added to retry delay
JITTER = 0.5

# For now, a pending job must be idle for at least 5 seconds
# before another worker can reclaim it.
MIN_IDLE_TIME = 5000

def initialize_consumer_group():
    try:
        redis_client.xgroup_create(
            name=STREAM_NAME,
            groupname=GROUP_NAME,
            id="0",
            mkstream=True
        )

        print("Consumer Group created successfully.")

    except ResponseError as error:
        if "BUSYGROUP" in str(error):
            print("Consumer Group already exists.")
        else:
            raise


if len(sys.argv) < 2:
    print("Usage: python -m app.worker <worker-name>")
    sys.exit(1)

CONSUMER_NAME = sys.argv[1]


def process_job(job):
    print("\n--- Processing Job ---")
    print("Job ID:", job["job_id"])
    print("Type:", job["type"])

    payload = json.loads(job["payload"])
    print("Payload:", payload)
    
    # slow job (use for worker crash)
    if job["type"] == "slow":
        time.sleep(30)

    # Temporary failure test
    if job["type"] == "fail":
        raise Exception("Intentional test failure")

    # Fail only on the first attempt.
    # Used to test the retry lifecycle.
    if (
        job["type"] == "fail_once"
        and int(job.get("attempt", "0")) == 0
    ):
        raise Exception("Intentional first-attempt failure")

    print("--- Job Completed ---\n")


def check_idempotency(job_id):
    """
    Check whether this job has already been successfully processed.
    """

    key = f"{IDEMPOTENCY_PREFIX}{job_id}"

    return redis_client.exists(key) == 1


def mark_job_processed(job_id):
    """
    Mark a job as successfully processed.
    """

    key = f"{IDEMPOTENCY_PREFIX}{job_id}"

    redis_client.set(
        key,
        "1"
    )


def handle_job(message_id, job):
    """
    Process a job and ACK it after successful processing.
    """

    print("Received message:", message_id)

    # 1. Idempotency check
    if check_idempotency(job["job_id"]):

        print(
            "Job already processed. "
            "Skipping:",
            job["job_id"]
        )

        # This delivery does not need to be processed again.
        redis_client.xack(
            STREAM_NAME,
            GROUP_NAME,
            message_id
        )

        return

    try:
        # 2. Get the current attempt number
        current_attempt = int(
            job.get("attempt", "0")
        )

        # 3. Mark job as processing in PostgreSQL
        #    and store the current attempt number.
        update_job_status(
            job["job_id"],
            "processing",
            current_attempt
        )

        print(
            "Job marked as processing:",
            job["job_id"]
        )

        print(
            "Current attempt:",
            current_attempt
        )

        # 4. Process the job
        process_job(job)
        
        jobs_completed.inc()

        # 5. Mark job as completed in PostgreSQL
        update_job_status(
            job["job_id"],
            "completed",
            current_attempt
        )

        print(
            "Job marked as completed:",
            job["job_id"]
        )

        # 6. Mark job as processed in Redis
        mark_job_processed(
            job["job_id"]
        )

        print(
            "Job marked as processed:",
            job["job_id"]
        )

        # ACK only after successful processing
        redis_client.xack(
            STREAM_NAME,
            GROUP_NAME,
            message_id
        )

        print(
            "Job ACKed:",
            message_id
        )

    except Exception as error:

        print(
            "Job failed:",
            error
        )
        
        jobs_failed.inc()

        # Make sure current_attempt exists
        # even if the failure happened before
        # attempt initialization.
        current_attempt = int(
            job.get("attempt", "0")
        )

        # Mark the current attempt as failed
        # in PostgreSQL.
        update_job_status(
            job["job_id"],
            "failed",
            current_attempt
        )

        # 7. Maximum attempts reached
        if current_attempt >= MAX_ATTEMPTS - 1:

            print(
                "Maximum attempts reached."
            )

            print(
                "Moving job to Dead Letter Queue."
            )
            
            # Mark job as permanently failed
            # in PostgreSQL.
            update_job_status(
                job["job_id"],
                "dead_letter",
                current_attempt
            )

            # Store permanently failed job
            # in the Dead Letter Queue.
            redis_client.xadd(
                DLQ_STREAM_NAME,
                {
                    "job_id": job["job_id"],
                    "type": job["type"],
                    "payload": job["payload"],
                    "attempt": job.get("attempt", "0"),
                    "reason": "maximum_attempts_exceeded"
                }
            )
            
            jobs_dead_lettered.inc()

            # Remove the original message
            # from the Pending Entries List.
            redis_client.xack(
                STREAM_NAME,
                GROUP_NAME,
                message_id
            )

            print(
                "Job moved to DLQ:",
                job["job_id"]
            )

            return

        # 8. Schedule next retry
        next_attempt = current_attempt + 1

        # Exponential backoff:
        #
        # attempt 1 -> 1 second
        # attempt 2 -> 2 seconds
        # attempt 3 -> 4 seconds
        # attempt 4 -> 8 seconds
        
        base_delay = (
            BASE_RETRY_DELAY
            * (2 ** (next_attempt - 1))
        )

        jitter = random.uniform(
            0,
            JITTER
        )

        retry_delay = base_delay + jitter

        retry_at = (
            time.time()
            + retry_delay
        )

        retry_job = {
            "job_id": job["job_id"],
            "type": job["type"],
            "payload": job["payload"],
            "attempt": str(next_attempt)
        }

        # Current execution is finished.
        redis_client.xack(
            STREAM_NAME,
            GROUP_NAME,
            message_id
        )

        # Put next attempt into retry Sorted Set.
        redis_client.zadd(
            RETRY_ZSET,
            {
                json.dumps(retry_job): retry_at
            }
        )
        
        jobs_retried.inc()

        print(
            "Retry scheduled:",
            next_attempt
        )

        print(
            "Retry delay:",
            retry_delay,
            "seconds"
        )

        print(
            "Retry time:",
            retry_at
        )
        
        print(
            "Base retry delay:",
            base_delay,
            "seconds"
        )

        print(
            "Jitter:",
            jitter,
            "seconds"
        )

        print(
            "Final retry delay:",
            retry_delay,
            "seconds"
        )


def reclaim_pending_jobs(start_id="0-0"):
    """
    Find pending jobs that have been idle long enough
    and transfer their ownership to this worker.
    """

    result = redis_client.xautoclaim(
        name=STREAM_NAME,
        groupname=GROUP_NAME,
        consumername=CONSUMER_NAME,
        min_idle_time=MIN_IDLE_TIME,
        start_id=start_id,
        count=10
    )

    return result


def main():
    
    initialize_consumer_group()
    next_start_id = "0-0"
    
    if next_start_id == "0-0":
        next_start_id = "0-0"

    print(
        "Cerberus worker started..."
    )

    print(
        f"Listening to stream: {STREAM_NAME}"
    )

    print(
        f"Consumer group: {GROUP_NAME}"
    )

    print(
        f"Consumer name: {CONSUMER_NAME}"
    )

    while True:

        # 1. Recover abandoned/pending jobs
        reclaimed = reclaim_pending_jobs(next_start_id)

        next_start_id = reclaimed[0]
        messages = reclaimed[1]

        for message_id, job in messages:

            print(
                "\n--- Reclaimed Pending Job ---"
            )

            print(
                "Message ID:",
                message_id
            )

            print(
                "Previous job owner has been replaced by:",
                CONSUMER_NAME
            )

            handle_job(
                message_id,
                job
            )

        # 2. Read new jobs
        messages = redis_client.xreadgroup(
            groupname=GROUP_NAME,
            consumername=CONSUMER_NAME,
            streams={
                STREAM_NAME: ">"
            },
            count=1,
            block=5000
        )

        for stream_name, entries in messages:

            for message_id, job in entries:

                handle_job(
                    message_id,
                    job
                )


if __name__ == "__main__":
    main()