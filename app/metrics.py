from prometheus_client import Counter

jobs_submitted = Counter(
    "cerberus_jobs_submitted_total",
    "Total number of jobs submitted to Cerberus"
)

jobs_completed = Counter(
    "cerberus_jobs_completed_total",
    "Total number of jobs completed successfully"
)

jobs_failed = Counter(
    "cerberus_jobs_failed_total",
    "Total number of jobs that failed"
)

jobs_retried = Counter(
    "cerberus_jobs_retried_total",
    "Total number of job retries scheduled"
)

jobs_dead_lettered = Counter(
    "cerberus_jobs_dead_lettered_total",
    "Total number of jobs moved to the dead letter queue"
)