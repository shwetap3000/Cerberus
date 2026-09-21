import json

from app.redis_client import redis_client

STREAM_NAME = "cerberus:jobs"

def process_job(job):
    print("\n--- Processing Job ---")
    print("Job ID:", job["job_id"])
    print("Type:", job["type"])

    payload = json.loads(job["payload"])
    print("Payload:", payload)

    print("--- Job Completed ---\n")


def main():
    last_id = "0-0"

    print("Cerberus worker started...")
    print(f"Listening to stream: {STREAM_NAME}")

    while True:
        messages = redis_client.xread(
            {STREAM_NAME: last_id},
            block=0,
            count=1
        )

        for stream_name, entries in messages:
            for message_id, job in entries:

                print("Received message:", message_id)

                process_job(job)

                last_id = message_id


if __name__ == "__main__":
    main()