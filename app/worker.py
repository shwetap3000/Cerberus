import json

from app.redis_client import redis_client

STREAM_NAME = "cerberus:jobs"
GROUP_NAME = "cerberus-workers"
CONSUMER_NAME = "worker-1"

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
            block=0
        )

        for stream_name, entries in messages:
            for message_id, job in entries:

                print("Received message:", message_id)

                process_job(job)


if __name__ == "__main__":
    main()