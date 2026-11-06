
from datetime import datetime, timedelta
import json
from pathlib import Path
import pandas as pd
import os
import boto3

import logging
import requests
from airflow.sdk import DAG, Param, get_current_context
from airflow.providers.standard.operators.python import PythonOperator

logger = logging.getLogger(__name__)

DATASET_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

OUTPUT_DIR = Path("/opt/airflow/data/bronze")


def extract_sample():
    context = get_current_context()

    batch_size = context["params"]["batch_size"]
    number_of_batches = context["params"]["number_of_batches"]

    logger.info(
        f"Starting extraction: "
        f"{batch_size=} {number_of_batches=}"
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for batch_number in range(number_of_batches):

        offset = batch_number * batch_size

        params = {
            "$limit": batch_size,
            "$offset": offset,
            "$select": "unique_key,created_date,closed_date,agency,complaint_type,borough,status",
            "$order": "created_date DESC,unique_key DESC",
        }

        response = requests.get(
            DATASET_URL,
            params=params,
            timeout=120,
        )

        response.raise_for_status()

        records = response.json()

        if not records:
            logger.info("No more records available.")
            break

        output_file = OUTPUT_DIR / f"batch_{batch_number + 1:05d}.json"

        with output_file.open("w", encoding="utf-8") as file:
            json.dump(records, file, ensure_ascii=False, indent=2)

        logger.info(
            f"Batch {batch_number + 1}: "
            f"{len(records)} records saved to {output_file}"
        )

    logger.info("Batch ingestion completed.")


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

    logger.info("Parquet conversion completed.")


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
    },

    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },

) as dag:

    extract_batches = PythonOperator(
        task_id="extract_sample",
        python_callable=extract_sample,
    )

    convert_batches = PythonOperator(
        task_id="convert_to_parquet",
        python_callable=convert_to_parquet,
    )

    upload_batches = PythonOperator(
    task_id="upload_to_r2",
    python_callable=upload_to_r2,
    )

    extract_batches >> convert_batches >> upload_batches