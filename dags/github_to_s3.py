
import json
import requests
import logging
import subprocess
import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from dotenv import load_dotenv
from pathlib import Path



load_dotenv()

BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
CHECKPOINT_KEY = os.getenv("AIRFLOW_CHECKPOINT_KEY")
PROJECT_ROOT = Path(__file__).resolve().parent.parent
def extract_data():
    url = "https://api.github.com/events"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        events = response.json()

        if not isinstance(events, list):
            raise ValueError("Unexpected API response format")

        if not events:
            raise ValueError("GitHub returned no events")

        logging.info("Successfully fetched %d events", len(events))

        return events

    except requests.exceptions.RequestException:
        logging.exception("GitHub API extraction failed")
        raise

    except ValueError:
        logging.exception("Invalid GitHub API response")
        raise

def get_previous_ids():
    s3 = S3Hook(aws_conn_id="aws_default")

    if not s3.check_for_key(
        key=CHECKPOINT_KEY,
        bucket_name=BUCKET_NAME
    ):
        logging.info("No checkpoint found. Starting fresh.")
        return set()

    content = s3.read_key(
        key=CHECKPOINT_KEY,
        bucket_name=BUCKET_NAME
    )

    checkpoint = json.loads(content)

    return set(checkpoint.get("processed_ids", []))


def deduplicate_events(ti):
    events = ti.xcom_pull(task_ids="extract")

    previous_ids = get_previous_ids()

    new_events = [
        event for event in events
        if event.get("id") and event["id"] not in previous_ids
    ]

    logging.info("New events: %d", len(new_events))
    logging.info("Duplicate events: %d", len(events) - len(new_events))

    return new_events


def validate_data(ti):
    events = ti.xcom_pull(task_ids="deduplicate")

    valid_events = []

    for event in events:
        if (
            event.get("type")
            and isinstance(event.get("repo"), dict)
            and event["repo"].get("name")
        ):
            valid_events.append(event)

    if not valid_events:
        logging.info("No new valid events to upload")
        return []

    logging.info("Validated %d new events", len(valid_events))

    return valid_events


def load_to_s3(ti):
    events = ti.xcom_pull(task_ids="validate")

    if not events:
        logging.info("Nothing new to upload")
        return None

    s3 = S3Hook(aws_conn_id="aws_default")

    object_key = (
        f"raw/github_events/"
        f"{datetime.now().strftime('%Y/%m/%d/%H%M%S%f')}.json"
    )

    s3.load_string(
        string_data=json.dumps(events),
        key=object_key,
        bucket_name=BUCKET_NAME,
        replace=True
    )

    logging.info("Uploaded %d events to %s", len(events), object_key)

    return object_key


def update_checkpoint(ti):
    events = ti.xcom_pull(task_ids="validate")

    if not events:
        logging.info("No new events. Checkpoint unchanged.")
        return

    previous_ids = get_previous_ids()

    new_ids = {event["id"] for event in events}

    # Keep a bounded set of recent IDs.
    updated_ids = list(previous_ids | new_ids)[-10000:]

    checkpoint = {
        "processed_ids": updated_ids,
        "updated_at": datetime.now().isoformat()
    }

    s3 = S3Hook(aws_conn_id="aws_default")

    s3.load_string(
        string_data=json.dumps(checkpoint, indent=4),
        key=CHECKPOINT_KEY,
        bucket_name=BUCKET_NAME,
        replace=True
    )

    logging.info("Checkpoint updated with %d IDs", len(updated_ids))

def process_delta_table(ti):
    s3_key = ti.xcom_pull(task_ids="load_to_s3")

    if not s3_key:
        logging.info("No new S3 file uploaded; skipping Delta processing.")
        return

    s3_uri = f"s3a://{BUCKET_NAME}/{s3_key}"

    venv_python = PROJECT_ROOT / ".venv" / "bin" / "python"
    python_executable = str(venv_python) if venv_python.exists() else sys.executable
    script_path = str(PROJECT_ROOT / "delta_lake" / "s3_to_delta.py")

    env = os.environ.copy()
    java_17_home = os.getenv("JAVA_17_HOME", "")
    if java_17_home and os.path.exists(java_17_home):
        env["JAVA_HOME"] = java_17_home
        env["PATH"] = f"{java_17_home}/bin:" + env.get("PATH", "")

    try:
        result = subprocess.run(
            [python_executable, script_path, s3_uri],
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )
        if result.stdout:
            logging.info(result.stdout)
    except subprocess.CalledProcessError as e:
        if e.stdout:
            logging.info("Spark STDOUT:\n%s", e.stdout)
        if e.stderr:
            logging.error("Spark STDERR:\n%s", e.stderr)
        raise
        
default_args = {
    "owner": "data-engineer",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}


with DAG(
    dag_id="github_to_s3",
    default_args=default_args,
    start_date=datetime(2026, 11, 23),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract_data,
    )

    deduplicate_task = PythonOperator(
        task_id="deduplicate",
        python_callable=deduplicate_events,
    )

    validate_task = PythonOperator(
        task_id="validate",
        python_callable=validate_data,
    )

    load_task = PythonOperator(
        task_id="load_to_s3",
        python_callable=load_to_s3,
    )

    checkpoint_task = PythonOperator(
        task_id="update_checkpoint",
        python_callable=update_checkpoint,
    )

    process_delta_task = PythonOperator(
    task_id="process_delta_table",
    python_callable=process_delta_table,
    execution_timeout=timedelta(minutes=30),
    retries=3,
)
    (
        extract_task
        >> deduplicate_task
        >> validate_task
        >> load_task
        >> checkpoint_task
        >> process_delta_task
    )
