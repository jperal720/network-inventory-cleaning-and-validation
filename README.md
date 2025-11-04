# Network-Inventory-Cleaning-and-Validation README

## Description

The purpose of this containerized-DAG-oriented pipeline is to cleanse and validate the observations stored in the inventory_raw.csv provided, solve its ambiguous cases and detect its anomalies –via an deepseek-api-powered FastAPI server– and export the results as inventory_clean.csv.

## Architecture Visualized

![pipeline_architecture](./imgs/data-cleansing-and-validation-architecture.png)

## To Run Pipeline

- Clone repository.

- Run docker-compose:

  - ```docker compose up -d --build```

- Once all the containers have started, execute the *inventory_pipeline* DAG found in ```localhost:8080```

- The results should be found in the directory *output/{date_of_execution}*

# Approach

Our pipeline is divided into four tasks and are all executed one after the other through an airflow DAG. The tasks execute in the following order:

```ingest_and_transform_inventory >>> llm_transform_owner >>> detect_anomalies >>> load_inventory```

Description of tasks:

- ```ingest_and_transform_inventory```: Ingests the *raw_inventory.csv* and applies deterministic transformations –e.g. validate ip, mac, etc– whose logic can be found in *src/clean_inventory.py*. The results are exported to *tmp/01-ingest-and-transform-inventory/{date_of_execution}_inventory_tmp.csv*.
- ```llm_transform_owner```: Transforms the ambiguous cases of the owner classes –owner, owner_team, and owner_email– using our deepseek-api-powered fastapi server. The results are first exported in a .json format in *tmp/02-llm-owner-transform/json_response/{date_of_execution}_response.json*, then they are set inside of our .csv, which is similarly found in *tmp/02-llm-owner-trasnform/{date_of_execution}_inventory_tmp.csv*.
- ```detect_anomalies```: Uses our deepseek-api-powered server to determine which observations are anomalies. The results can be found in *outputs/{date_of_exectution}/anomalies.json*.
- ```load_inventory```: Exports the final clean inventory to *outputs/{date_of_execution}/clean_inventory.csv*.

# Prompts

# Cons
