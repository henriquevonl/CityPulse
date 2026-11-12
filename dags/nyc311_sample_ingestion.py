
from datetime import datetime, timedelta
import json
from pathlib import Path
import pandas as pd
import os
import boto3
import time
import random

import logging
import requests
from airflow.sdk import DAG, Param, get_current_context
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
)

logger = logging.getLogger(__name__)

DATASET_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

OUTPUT_DIR = Path("/opt/airflow/data/bronze")


def test_export():
    url = "https://data.cityofnewyork.us/api/v3/views/erm2-nwe9/export.csv"

    params = {
        "query": (
            "SELECT unique_key, created_date, closed_date, "
            "agency, complaint_type, borough, status "
            "WHERE created_date >= '2026-11-01T00:00:00' "
            "AND created_date < '2026-11-02T00:00:00'"
        )
    }

    headers = {
        "X-App-Token": os.environ["SOCRATA_APP_TOKEN"]
    }

    response = requests.get(
        url,
        params=params,
        headers=headers,
        timeout=600,
    )

    response.raise_for_status()

    output_file = OUTPUT_DIR / "nyc311_export.csv"

    with output_file.open("wb") as file:
        file.write(response.content)

    logger.info(f"Export saved to {output_file}")


def extract_sample():
    context = get_current_context()
    start_date = context["params"]["start_date"]
    end_date = context["params"]["end_date"]

    batch_size = context["params"]["batch_size"]
    number_of_batches = context["params"]["number_of_batches"]

    logger.info(
        f"Starting extraction: "
        f"{batch_size=} {number_of_batches=}"
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    checkpoint_file = OUTPUT_DIR / "checkpoint.json"

    # Initial values for a new extraction
    batch_number = 0
    last_created_date = None
    last_unique_key = None

    # Restore progress if a checkpoint exists
    if checkpoint_file.exists():
        with checkpoint_file.open("r", encoding="utf-8") as file:
            checkpoint = json.load(file)

        last_created_date = checkpoint["last_created_date"]
        last_unique_key = checkpoint["last_unique_key"]
        batch_number = checkpoint["batch_number"]

        logger.info(
            f"Resuming from batch {batch_number}: "
            f"{last_created_date}, unique_key={last_unique_key}"
        )

    while True:

        where_clause = (
            f"created_date >= '{start_date}T00:00:00' "
            f"AND created_date < '{end_date}T00:00:00'"
        )

        if last_created_date is not None:
            where_clause += (
                f" AND (created_date > '{last_created_date}' "
                f"OR (created_date = '{last_created_date}' "
                f"AND unique_key > '{last_unique_key}'))"
            )

        params = {
            "$limit": batch_size,
            "$where": where_clause,
            "$select": "unique_key,created_date,closed_date,agency,complaint_type,borough,status",
            "$order": "created_date ASC,unique_key ASC",
        }

        headers = {
            "X-App-Token": os.environ["SOCRATA_APP_TOKEN"]
        }

        response = request_with_retry(
            DATASET_URL,
            params=params,
            headers=headers
        )
        
        response.raise_for_status()

        records = response.json()

        if not records:
            logger.info("No more records available.")
            break

        batch_number += 1

        # Save the batch first
        output_file = OUTPUT_DIR / f"batch_{batch_number:05d}.json"
        
        with output_file.open("w", encoding="utf-8") as file:
            json.dump(records, file, ensure_ascii=False, indent=2)

        # Update cursor after successful batch write
        last_record = records[-1]
        last_created_date = last_record["created_date"]
        last_unique_key = last_record["unique_key"]

        # Save checkpoint
        checkpoint = {
            "batch_number": batch_number,
            "last_created_date": last_created_date,
            "last_unique_key": last_unique_key,
        }

        with checkpoint_file.open("w", encoding="utf-8") as file:
            json.dump(checkpoint, file, indent=2)

        logger.info("Checkpoint saved.")

        logger.info(
            f"Batch {batch_number}: "
            f"{len(records)} records saved to {output_file}"
        )

    logger.info("Batch ingestion completed.")


def request_with_retry(url, params, headers, max_retries=5):
    for attempt in range(max_retries + 1):
        response = requests.get(
            url,
            params=params,
            headers=headers,
            timeout=120,
        )

        if response.status_code not in (429, 503):
            response.raise_for_status()
            return response

        if attempt == max_retries:
            response.raise_for_status()

        wait_seconds = min(60, 2 ** attempt) + random.uniform(0, 1)

        logger.warning(
            f"HTTP {response.status_code}. "
            f"Retrying in {wait_seconds:.1f} seconds..."
        )

        time.sleep(wait_seconds)


def convert_csv_to_parquet():
    csv_file = OUTPUT_DIR / "nyc311_export.csv"
    parquet_file = OUTPUT_DIR / "nyc311_export.parquet"

    df = pd.read_csv(csv_file)

    df.to_parquet(parquet_file, index=False)

    # Read the Parquet back
    df_parquet = pd.read_parquet(parquet_file)

    logger.info(
        f"Converted {len(df)} records "
        f"from CSV to Parquet: {parquet_file}"
    )


def convert_to_parquet():
    json_files = sorted(OUTPUT_DIR.glob("batch_*.json"))

    parquet_dir = OUTPUT_DIR / "parquet"
    parquet_dir.mkdir(parents=True, exist_ok=True)

    for json_file in json_files:
        with json_file.open("r", encoding="utf-8") as file:
            records = json.load(file)

        df = pd.DataFrame(records)

        parquet_file = parquet_dir / f"{json_file.stem}.parquet"

        df.to_parquet(parquet_file, index=False)

        logger.info(f"Converted {json_file.name} -> {parquet_file.name}")

    logger.info("Parquet file conversion completed.")


def upload_one_parquet_to_r2():
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["R2_ENDPOINT_URL"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )

    bucket = os.environ["R2_BUCKET_NAME"]

    parquet_file = OUTPUT_DIR / "nyc311_export.parquet"

    object_key = "bronze/nyc311/nyc311_export.parquet"

    s3.upload_file(
        str(parquet_file),
        bucket,
        object_key,
    )

    logger.info(f"Uploaded: {object_key}")


def upload_to_r2():
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["R2_ENDPOINT_URL"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )

    bucket = os.environ["R2_BUCKET_NAME"]

    parquet_dir = OUTPUT_DIR / "parquet"

    for parquet_file in parquet_dir.glob("*.parquet"):
        object_key = f"bronze/nyc311/{parquet_file.name}"

        s3.upload_file(
            str(parquet_file),
            bucket,
            object_key,
        )

        logger.info(f"Uploaded: {object_key}")

    logger.info("R2 upload completed.")


with DAG(
    dag_id="nyc311_sample_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["citypulse", "ingestion"],

    params={
        "batch_size": Param(
            1000,
            type="integer",
            minimum=1,
            maximum=50000,
        ),
        "number_of_batches": Param(
            5,
            type="integer",
            minimum=1,
            maximum=100,
        ),
        "start_date": Param(
            "2026-11-01",
            type="string",
        ),
        "end_date": Param(
            "2026-11-02",
            type="string",
        ),
    },

    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },

) as dag:

    # extract_batches = PythonOperator(
    #     task_id="extract_sample",
    #     python_callable=extract_sample,
    # )


    convert_batches = PythonOperator(
            task_id="convert_csv_to_parquet",
            python_callable=convert_csv_to_parquet,
        )


    # convert_batches = PythonOperator(
    #     task_id="convert_to_parquet",
    #     python_callable=convert_to_parquet,
    # )


    upload_batches = PythonOperator(
            task_id="upload_one_parquet_to_r2",
            python_callable=upload_one_parquet_to_r2,
        )

    # upload_batches = PythonOperator(
    #     task_id="upload_to_r2",
    #     python_callable=upload_to_r2,
    # )

    run_databricks_job = DatabricksRunNowOperator(
        task_id="run_databricks_job",
        job_id=930909712167983,
        databricks_conn_id="databricks_default",
        wait_for_termination=True,
        polling_period_seconds=30,
    )

    test_export_task = PythonOperator(
        task_id="test_export",
        python_callable=test_export,
    )

    test_export_task >> convert_batches >> upload_batches >> run_databricks_job

    # extract_batches >> convert_batches >> upload_batches >> run_databricks_job