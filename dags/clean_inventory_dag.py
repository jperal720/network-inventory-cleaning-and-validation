from datetime import datetime as dt
from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import pandas as pd
import shutil
import sys
import os

SRC_PATH = os.path.join(os.path.dirname(__file__), '..', 'src')
UTILS_PATH = os.path.join(os.path.dirname(__file__), 'utils')

sys.path.append(SRC_PATH)
sys.path.append(UTILS_PATH)

from clean_inventory import transform_inventory
from utils.owner_transform_api_call import call_process_file
from utils.anomaly_detector_api_call import call_anomaly_detector

# ------------------------------------------------------------------
# DAG definition
# ------------------------------------------------------------------

with DAG(
    dag_id="inventory_pipeline",
    start_date=dt(2025, 10, 27),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        "owner": "data-eng",
        "retries": 0,
    },
    tags=["inventory", "cleaning", "spark-cluster"],
) as dag:
    
    # Upload to minIO
    INPUT_PATH = os.path.join("s3a://", "inventory", "inventory_raw.csv")
    TRANSFORM_MINIO_PATH = os.path.join("s3a://", "inventory", "tmp", "01-ingest-and-transform-inventory")
    FILE_NAME = f"{dt.today().year}-{dt.today().day}-{dt.today().month}_inventory_tmp"
    
    spark_clean_inventory = SparkSubmitOperator(
        task_id="spark_ingest_and_transform_inventory",
        application=f"{SRC_PATH}/spark_clean_inventory.py",
        conn_id="spark_connection",
        java_class=None,
        total_executor_cores="2",
        executor_memory="2g",
        driver_memory="1g",
        num_executors="1",
        name="data_cleansing",
        verbose=True,
        application_args=[
            "--input-path", INPUT_PATH,
            "--output-dir", TRANSFORM_MINIO_PATH,
            "--file-name", FILE_NAME
        ],
        py_files=os.path.join(SRC_PATH, 'utils', 'InventoryTransformations.py'),
        do_xcom_push=False,
        packages="software.amazon.awssdk:bundle:2.23.19,org.apache.hadoop:hadoop-aws:3.4.0"
    )
    
    @task()
    def llm_transform_owner(tmp_csv_path: str):
        """
        Run the deepseek-powered owner transformation,
        and return the file path
        """

        csv_name = call_process_file(tmp_csv_path)

        if csv_name is None:
            print("API call has failed; owner classes have not been transformed")
            return tmp_csv_path
        
        return os.path.join(os.path.dirname(__file__), '..', 'tmp', '02-llm-owner-transform', csv_name)
    
    @task()
    def detect_anomalies(clean_file_path: str):
        """
        Run deepseek-powered anomaly detector and push anomaly.json to output path
        """

        OUTPUT_PATH=os.path.join(os.path.dirname(__file__), '..', 'outputs')
        DESTINATION_PATH = os.path.join(OUTPUT_PATH, f"{clean_file_path.split('/')[-1].split('_')[0]}")

        json_name = call_anomaly_detector(clean_file_path)
        os.makedirs(DESTINATION_PATH, exist_ok=True)

        if json_name is None:
            print("Could not retrieve *_anomalies.json")
            return DESTINATION_PATH
        
        # Copying anomalies.json response from tmp directory to outputs/{date} directory
        shutil.copy(os.path.join(os.path.dirname(__file__), '..', 'tmp', '03-anomaly-detector', 'json_response', f"{json_name}"),
                    os.path.join(DESTINATION_PATH, 'anomalies.json'))
        
        print(f"Successfully extracted anomalies.json, and can now be found in {DESTINATION_PATH}")
        return DESTINATION_PATH
        


    @task()
    def load_inventory(clean_file_path: str, output_dir: str):
        """
        Reading final csv and pushing it to output path
        """
        df_clean = pd.read_csv(clean_file_path, index_col=0)
        df_clean.to_csv(os.path.join(output_dir, 'inventory_clean.csv'))

        print(f"Rows in clean inventory: {len(df_clean)}")

    # wiring
    # cleaned_path = ingest_and_transform_inventory()
    llm_transformation_path = llm_transform_owner(os.path.join(TRANSFORM_MINIO_PATH, FILE_NAME))
    output_dir = detect_anomalies(llm_transformation_path)
    load_task = load_inventory(llm_transformation_path, output_dir)

    spark_clean_inventory >> llm_transformation_path >> output_dir >> load_task