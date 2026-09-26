import json
import sys

from app.redis_client import redis_client

STREAM_NAME = "cerberus:jobs"
GROUP_NAME = "cerberus-workers"

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

    print("--- Job Completed ---\n")


def main():
    print("Cerberus worker started...")
    print(f"Listening to stream: {STREAM_NAME}")
    print(f"Consumer group: {GROUP_NAME}")
    print(f"Consumer name: {CONSUMER_NAME}")

    while True:

        messages = redis_client.xreadgroup(
            groupname=GROUP_NAME,
            consumername=CONSUMER_NAME,
            streams={STREAM_NAME: ">"},
            count=1,
            block=5000
        )

        for stream_name, entries in messages:

            for message_id, job in entries:

                print("Received message:", message_id)

                try:
                    process_job(job)

                    redis_client.xack(
                        STREAM_NAME,
                        GROUP_NAME,
                        message_id
                    )

                    print("Job ACKed:", message_id)

                except Exception as error:
                    print("Job failed:", error)


if __name__ == "__main__":
    main()