from datetime import datetime
from airflow import DAG
from airflow.decorators import task

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
        DESTINATION_PATH = os.path.join(OUTPUT_PATH, f'{clean_file_path.split('/')[-1].split('_')[0]}')

        json_name = call_anomaly_detector(clean_file_path)
        os.makedirs(DESTINATION_PATH, exist_ok=True)

        if json_name is None:
            print("Could not retrieve *_anomalies.json")
            return DESTINATION_PATH
        
        # Copying anomalies.json response from tmp directory to outputs/{date} directory
        shutil.copy(os.path.join(os.path.dirname(__file__), '..', 'tmp', '03-anomaly-detector', 'json_response', f'{json_name}'),
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
    cleaned_path = ingest_and_transform_inventory()
    llm_transformation_path = llm_transform_owner(cleaned_path)
    output_dir = detect_anomalies(llm_transformation_path)
    load_inventory(llm_transformation_path, output_dir)