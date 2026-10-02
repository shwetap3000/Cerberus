import json
import sys
import time

from app.redis_client import redis_client

STREAM_NAME = "cerberus:jobs"
GROUP_NAME = "cerberus-workers"

RETRY_ZSET = "cerberus:retries"
DLQ_STREAM_NAME = "cerberus:dlq"

BASE_RETRY_DELAY = 1
MAX_ATTEMPTS = 5

# For now, a pending job must be idle for at least 5 seconds
# before another worker can reclaim it.
MIN_IDLE_TIME = 5000

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

    # Temporary failure test
    if job["type"] == "fail":
        raise Exception("Intentional test failure")

    print("--- Job Completed ---\n")

def handle_job(message_id, job):
    """
    Process a job and ACK it after successful processing.
    """

    print("Received message:", message_id)

    try:
        # Try to process the job
        process_job(job)

        # Processing succeeded, so ACK the message
        redis_client.xack(
            STREAM_NAME,
            GROUP_NAME,
            message_id
        )

        print("Job ACKed:", message_id)

    except Exception as error:
        print("Job failed:", error)

        current_attempt = int(job.get("attempt", "0"))

        # Maximum number of total executions has been reached
        if current_attempt >= MAX_ATTEMPTS - 1:
            print("Maximum attempts reached.")
            print("Moving job to Dead Letter Queue.")

            # Store the permanently failed job
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

            # The original message is no longer being processed,
            # so remove it from the Pending Entries List.
            redis_client.xack(
                STREAM_NAME,
                GROUP_NAME,
                message_id
            )

            print("Job moved to DLQ:", job["job_id"])
            return

        # Schedule the next attempt
        next_attempt = current_attempt + 1

        # Exponential backoff:
        # attempt 1 -> 1 second
        # attempt 2 -> 2 seconds
        # attempt 3 -> 4 seconds
        # attempt 4 -> 8 seconds
        retry_delay = BASE_RETRY_DELAY * (2 ** (next_attempt - 1))

        retry_at = time.time() + retry_delay

        retry_job = {
            "job_id": job["job_id"],
            "type": job["type"],
            "payload": job["payload"],
            "attempt": str(next_attempt)
        }

        # The current execution is finished.
        # ACK the current Redis Stream message.
        redis_client.xack(
            STREAM_NAME,
            GROUP_NAME,
            message_id
        )

        # Put the next attempt into the retry Sorted Set.
        redis_client.zadd(
            RETRY_ZSET,
            {
                json.dumps(retry_job): retry_at
            }
        )

        print("Retry scheduled:", next_attempt)
        print("Retry delay:", retry_delay, "seconds")
        print("Retry time:", retry_at)

def reclaim_pending_jobs():
    """
    Find pending jobs that have been idle long enough
    and transfer their ownership to this worker.
    """

    result = redis_client.xautoclaim(
        name=STREAM_NAME,
        groupname=GROUP_NAME,
        consumername=CONSUMER_NAME,
        min_idle_time=MIN_IDLE_TIME,
        start_id="0-0",
        count=10
    )

    return result

def main():
    print("Cerberus worker started...")
    print(f"Listening to stream: {STREAM_NAME}")
    print(f"Consumer group: {GROUP_NAME}")
    print(f"Consumer name: {CONSUMER_NAME}")

    while True:

        # 1. Recover abandoned/pending jobs
        reclaimed = reclaim_pending_jobs()

        next_start_id = reclaimed[0]
        messages = reclaimed[1]

        for message_id, job in messages:

            print("\n--- Reclaimed Pending Job ---")
            print("Message ID:", message_id)
            print(
                "Previous job owner has been replaced by:",
                CONSUMER_NAME
            )

            handle_job(message_id, job)

        # 2. Read new jobs
        messages = redis_client.xreadgroup(
            groupname=GROUP_NAME,
            consumername=CONSUMER_NAME,
            streams={STREAM_NAME: ">"},
            count=1,
            block=5000
        )

        for stream_name, entries in messages:

            for message_id, job in entries:
                handle_job(message_id, job)

if __name__ == "__main__":
    main()