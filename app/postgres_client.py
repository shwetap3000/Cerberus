import os
import uuid

import psycopg
from dotenv import load_dotenv
from psycopg.types.json import Jsonb
from urllib.parse import urlsplit, urlunsplit

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
DB_HOST = os.getenv("DB_HOST", "localhost")



def get_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    
    parts = urlsplit(DATABASE_URL)

    docker_database_url = urlunsplit((
        parts.scheme,
        f"{parts.username}:{parts.password}@{DB_HOST}:{parts.port}",
        parts.path,
        parts.query,
        parts.fragment
    ))

    return psycopg.connect(docker_database_url)


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

if __name__ == "__main__":
    test_job_id = uuid.uuid4()

    create_job(
        test_job_id,
        "test",
        {"message": "Hello from Cerberus"}
    )

    print("Test job inserted successfully")