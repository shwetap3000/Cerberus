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

def update_job_status(job_id, status, attempts=None):
    connection = get_connection()

    try:
        with connection.cursor() as cursor:

            if attempts is not None:
                cursor.execute(
                    """
                    UPDATE jobs
                    SET status = %s,
                        attempts = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE job_id = %s
                    """,
                    (
                        status,
                        attempts,
                        job_id
                    )
                )
            else:
                cursor.execute(
                    """
                    UPDATE jobs
                    SET status = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE job_id = %s
                    """,
                    (
                        status,
                        job_id
                    )
                )

        connection.commit()

    finally:
        connection.close()

# if __name__ == "__main__":
#     test_job_id = uuid.uuid4()

#     create_job(
#         test_job_id,
#         "test",
#         {"message": "Hello from Cerberus"}
#     )

#     print("Test job inserted successfully")


# temporary replacing (for testing postgresql before involving the worker)
if __name__ == "__main__":
    test_job_id = "e06034f2-7899-4d4f-9ab8-f833f049faeb"    # job_id from pgAdmin data output

    update_job_status(
        test_job_id,
        "processing"
    )

    print("Job status updated successfully")