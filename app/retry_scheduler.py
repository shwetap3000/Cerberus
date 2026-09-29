import time

from app.redis_client import redis_client

RETRY_ZSET = "cerberus:retries"

def get_ready_retries():
    current_time = time.time()

    ready_jobs = redis_client.zrangebyscore(
        RETRY_ZSET,
        0,
        current_time
    )

    return ready_jobs

def main():
    print("Cerberus retry scheduler started...")

    while True:
        ready_jobs = get_ready_retries()

        print("Ready retries:", len(ready_jobs))

        time.sleep(1)

if __name__ == "__main__":
    main()