import os
import uuid

import psycopg
from dotenv import load_dotenv
from psycopg.types.json import Jsonb

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")


def get_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")

    return psycopg.connect(DATABASE_URL)


def create_job(job_id, job_type, payload):
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO jobs (
                    job_id,
                    job_type,
                    payload
                )
                VALUES (%s, %s, %s);
                """,
                (
                    job_id,
                    job_type,
                    Jsonb(payload)
                )
            )

        connection.commit()

    finally:
        connection.close()


if __name__ == "__main__":
    test_job_id = uuid.uuid4()

    create_job(
        test_job_id,
        "test",
        {"message": "Hello from Cerberus"}
    )

    print("Test job inserted successfully")