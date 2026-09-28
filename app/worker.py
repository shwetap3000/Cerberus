import json
import sys

from app.redis_client import redis_client


STREAM_NAME = "cerberus:jobs"
GROUP_NAME = "cerberus-workers"

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
        # The worker is alive, but the job execution failed
        print("Job failed:", error)

        # Get the current attempt number
        current_attempt = int(job.get("attempt", "0"))

        # Increase the attempt number for the retry
        next_attempt = current_attempt + 1

        # ACK the original failed delivery
        redis_client.xack(
            STREAM_NAME,
            GROUP_NAME,
            message_id
        )

        # Create a new Redis Stream message for the retry
        redis_client.xadd(
            STREAM_NAME,
            {
                "job_id": job["job_id"],
                "type": job["type"],
                "payload": job["payload"],
                "attempt": str(next_attempt)
            }
        )

        print("Retry scheduled:", next_attempt)


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
            print("Previous job owner has been replaced by:", CONSUMER_NAME)

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