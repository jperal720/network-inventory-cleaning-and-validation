from datetime import datetime
from airflow import DAG
from airflow.decorators import task

import pandas as pd
import sys
import os

SRC_PATH = os.path.join(os.path.dirname(__file__), '..', 'src')
print(SRC_PATH)

sys.path.append(SRC_PATH)

from clean_inventory import transform_inventory

# ------------------------------------------------------------------
# DAG definition
# ------------------------------------------------------------------

with DAG(
    dag_id="inventory_pipeline",
    start_date=datetime(2025, 10, 27),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        "owner": "data-eng",
        "retries": 0,
    },
    tags=["inventory", "cleaning"],
) as dag:

    @task()
    def ingest_and_transform_inventory(execution_date=None):
        """
        Run the transform, save {date}_inventory_clean.csv,
        and return (push via XCom) the file path.
        """
        # Use Airflow's logical date for deterministic naming
        run_date_str = execution_date.strftime("%Y-%m-%d")
        # inventory_raw.csv path 
        INPUT_PATH = os.path.join(os.path.dirname(__file__), '..', 'inputs', 'inventory_raw.csv')

        output_path = transform_inventory(INPUT_PATH)
        
        return output_path

    @task()
    def load_inventory(clean_file_path: str):
        """
        Example downstream task.
        You now know exactly which file to read.
        """
        df_clean = pd.read_csv(clean_file_path)
        OUTPUT_PATH=os.path.join(os.path.dirname(__file__), '..', 'outputs', f'{clean_file_path.split('/')[-1].split('_')[0]}_inventory_clean.csv')
        df_clean.to_csv(OUTPUT_PATH)

        print(f"Rows in clean inventory: {len(df_clean)}")

    # wiring
    cleaned_path = ingest_and_transform_inventory()
    load_inventory(cleaned_path)