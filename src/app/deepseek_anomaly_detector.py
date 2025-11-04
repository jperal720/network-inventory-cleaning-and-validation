import os
import sys
import json
import pandas as pd

from datetime import datetime as dt
from openai import OpenAI


def call_llm(df: str, secrets_path: str):
    with open(secrets_path, 'r') as file:
        secrets = json.load(file)

    json_df = df.to_json()


    SYSTEM_PROMPT = """You are a helpful assistant. Do not answer with more words than you need;
      when a user asks you something you do it as exact as possible."""
    
    # observation source_row_id, affected_fields, issue_type, recommended_action, anomaly_confidence
    USER_PROMPT = """
    I want you to exclusively look at each observation in the attached .json file" .
    Return to me which rows you think are anomalies; return your results in a .json file.

    In the .json file that you return each row of interest should have the following classes: source_row_id (this is determined by the source_row_id class of each observation), affected_fields (these are determined by which classes you think make that specific observation an anomaly), issue_type (what you think is the issue with respects to each affected_field), recommended_action (this should be a binary value, either "modify" or "drop"), and anomaly_confidence (from 0 to 100 how sure you are that the observation is an anomaly) 

    EXAMPLE JSON OUTPUT: {
        "source_row_id": 0,
        "affected_fields": [ip_valid, mac_valid, reverse_ptr, fqdn, device_type],
        "issue_type": "ip is invalid, mac is invalid, reverse_ptr does not exist, fqdn does not exist, and device_type is not listed. The combination of all of these make this an anomaly.",
        "recommended_action": "drop",
        "anomaly_confidence": 75
    }, 
    ...,
    {
        "source_row_id": 10,
        ...,
        "recommended_action": "modify",
        "anomaly_confidence": 100
    }
    """


    client = OpenAI(api_key=secrets['deepseek_api_key'], base_url="https://api.deepseek.com")

    response = client.chat.completions.create(
        model="deepseek-reasoner",
        messages=[
            {"role": "system", "content": f"{SYSTEM_PROMPT}"},
            {"role": "user", "content": f"json_file: {json_df} {USER_PROMPT}"},
        ],
        response_format={
            "type": "json_object"
        },
        stream=False,
        temperature=0.2
    )

    response_json = json.loads(response.choices[0].message.content)

    return response_json


def run_anomaly_detector(df_path: str) -> dict:
    """
    High-level pipeline:
    - read CSV
    - send subset to LLM
    - save model response JSON and merged CSV in tmp/
    - return both in memory for API response
    """
    # prep paths
    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
    SECRETS_PATH = os.path.join(ROOT_DIR, '..', '..', 'secrets.json')

    df_tmp = pd.read_csv(df_path, index_col=0)

    # build df_owner subset (owner cols only)
    df_anomaly = pd.DataFrame()
    df_anomaly[['source_row_id', 'ip', 'ip_valid', 'ip_type', 'subnet_cidr', 'hostname', 'hostname_valid', 'fqdn', 'reverse_ptr', 'mac', 'mac_valid', 'device_type', 'site_normalized']] = df_tmp[['source_row_id', 'ip', 'ip_valid', 'ip_type', 'subnet_cidr', 'hostname', 'hostname_valid', 'fqdn', 'reverse_ptr', 'mac', 'mac_valid', 'device_type', 'site_normalized']]

    # call model
    response_json = call_llm(df_anomaly, secrets_path=SECRETS_PATH)

    # save artifacts
    CURR_DATE = f"{dt.today().year}-{dt.today().day}-{dt.today().month}"

    TMP_PATH = os.path.join(ROOT_DIR, '..', '..', 'tmp', '03-anomaly-detector')
    os.makedirs(os.path.join(TMP_PATH, 'json_response'), exist_ok=True)

    RESPONSE_PATH = os.path.join(TMP_PATH, 'json_response', f"{CURR_DATE}_anomaly_response.json")
    with open(RESPONSE_PATH, 'w') as f:
        json.dump(response_json, f)

    return {
        "anomaly_response_path": RESPONSE_PATH,
        "llm_json": response_json
    }