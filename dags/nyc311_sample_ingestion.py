
from datetime import datetime
import json
from pathlib import Path
import pandas as pd

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

DATASET_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

OUTPUT_DIR = Path("/opt/airflow/data/bronze")


BATCH_SIZE = 1000
NUMBER_OF_BATCHES = 3


def extract_sample():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for batch_number in range(NUMBER_OF_BATCHES):

        offset = batch_number * BATCH_SIZE

        params = {
            "$limit": BATCH_SIZE,
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
            print("No more records available.")
            break

        output_file = OUTPUT_DIR / f"batch_{batch_number + 1:05d}.json"

        with output_file.open("w", encoding="utf-8") as file:
            json.dump(records, file, ensure_ascii=False, indent=2)

        print(
            f"Batch {batch_number + 1}: "
            f"{len(records)} records saved to {output_file}"
        )

    print("Batch ingestion completed.")


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

        print(f"Converted {json_file.name} -> {parquet_file.name}")

    print("Parquet conversion completed.")


with DAG(
    dag_id="nyc311_sample_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["citypulse", "ingestion"],
) as dag:

    extract_batches = PythonOperator(
        task_id="extract_sample",
        python_callable=extract_sample,
    )

    convert_batches = PythonOperator(
        task_id="convert_to_parquet",
        python_callable=convert_to_parquet,
    )

    extract_batches >> convert_batches