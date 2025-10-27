from datetime import datetime
from airflow import DAG
from airflow.decorators import task

import pandas as pd
import os

# ------------------------------------------------------------------
# This would normally live in a separate module you import.
# I'm inlining it here so it's easy to see the flow.
# ------------------------------------------------------------------

def transform_inventory():
    """
    Do all your pandas munging here and return the cleaned dataframe.
    """
    # EXAMPLE ONLY: replace this with your real logic
    df_raw = pd.read_csv("/opt/airflow/data/inventory_raw.csv")

    # ... your cleaning steps ...
    df_clean = df_raw.copy()  # placeholder for your real transforms

    return df_clean


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
    def clean_inventory(execution_date=None):
        """
        Run the transform, save {date}_inventory_clean.csv,
        and return (push via XCom) the file path.
        """
        # Use Airflow's logical date for deterministic naming
        run_date_str = execution_date.strftime("%Y-%m-%d")

        df_clean = transform_inventory()

        output_dir = "/opt/airflow/data/clean"
        os.makedirs(output_dir, exist_ok=True)

        output_path = os.path.join(
            output_dir,
            f"{run_date_str}_inventory_clean.csv"
        )

        df_clean.to_csv(output_path, index=False)

        # Whatever this function returns becomes an XCom that
        # the next task can pull.
        return output_path

    @task()
    def consume_inventory(clean_file_path: str):
        """
        Example downstream task.
        You now know exactly which file to read.
        """
        df_clean = pd.read_csv(clean_file_path)

        # Do the next thing:
        # - quality checks
        # - load to warehouse
        # - create summary, etc.
        print(f"Rows in clean inventory: {len(df_clean)}")

    # wiring
    cleaned_path = clean_inventory()
    consume_inventory(cleaned_path)