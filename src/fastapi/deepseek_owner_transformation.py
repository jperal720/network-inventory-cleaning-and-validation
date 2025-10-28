import os
import sys
import json
import pandas as pd

from datetime import datetime as dt
from openai import OpenAI

def transform_and_save_df(response_json_path: str, df_tmp: pd.DataFrame, save_path: str):

    # LOADING JSON INTO DATAFRAME
    df_response = pd.read_json(response_json_path)

    # DETERMINING HIGH CONFIDENCE CHANGES
    high_confidence_indeces = df_response[df_response['confidence_percentage'] >= 70].index

    # KEEPING OBSERVATIONS WITH A CONFIDENE >= 70.
    df_tmp.loc[high_confidence_indeces, ['owner', 'owner_email', 'owner_team']] = df_response.loc[high_confidence_indeces, ['owner', 'owner_email', 'owner_team']] 

    # SAVING DATAFRAME
    df_tmp.to_csv(save_path)





def main(df_path: str):
    SECRETS_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'secrets.json')
    with open(SECRETS_PATH, 'r') as file:
        secrets = json.load(file)

    df_tmp = pd.read_csv(df_path, index_col=0)
    df_owner = pd.DataFrame()
    df_owner[['owner', 'owner_team', 'owner_email']] = df_tmp[['owner', 'owner_team', 'owner_email']]
    json_owner = df_owner.to_json()


    SYSTEM_PROMPT = """You are a helpful assistant. Do not answer with more words than you need;
      when a user asks you something you do it as exact as possible.
      
      EXAMPLE JSON OUTPUT: {
        "question": "please provide a .json file with a column named x and its value is "content"
        "answer": {
            "x": "content"
        }
      }"""
    
    USER_PROMPT = """
    I want you to exclusively look at the owner, owner_team, and owner_email per each observation in the attached .json file" .
    Return to me how you think they should be parsed, regardless of whether you agree with the original parsing or not,
    and return your parsing in a .json file. That is, if you think the owner value in an observation should be swapped with
    the value in owner_team, swap them.

    Consider taking into account that the name of teams (ops, platform, facilities, etc.) should be in the owner_team column,
    and only peoples' names should be in the owner column.
      
    Additionally, include a new column of your confidence_percentage (from 0 to 100%) that these changes are correct.
    I only would like the .json from you (no need to explain your reasoning).

    If you think that an observation doesn't require any changing, that's fine, too.

    Note: If you think a value is null, just put null; don't change the format.

    EXAMPLE JSON OUTPUT: {
        "owner": {
            "0": "juan",
            ...,
            "10": null,
            ...
        },
        "owner_team": {
            "0": "marketing",
            ...,
            "14": null
        },
        "owner_email":{
            "0": "juan@example.com"
            ...
        },
        "confidence_percentage":{
            "0": 75,
            ...
        }
    }
    """


    client = OpenAI(api_key=secrets['deepseek_api_key'], base_url="https://api.deepseek.com")

    response = client.chat.completions.create(
        model="deepseek-reasoner",
        messages=[
            {"role": "system", "content": f"{SYSTEM_PROMPT}"},
            {"role": "user", "content": f"json_file: {json_owner} {USER_PROMPT}"},
        ],
        response_format={
            "type": "json_object"
        },
        stream=False
    )

    response_json = json.loads(response.choices[0].message.content)
    CURR_DATE = f"{dt.today().year}-{dt.today().day}-{dt.today().month}"

    TMP_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'tmp', '02-llm-owner-transform')
    RESPONSE_PATH = f'{TMP_PATH}/json_response/{CURR_DATE}_response.json'

    with open(RESPONSE_PATH, 'w') as file:
        json.dump(response_json, file)

    transform_and_save_df(RESPONSE_PATH, df_tmp=df_tmp, save_path=os.path.join(TMP_PATH, f"{CURR_DATE}_inventory_tmp.csv"))






if __name__ == "__main__":
    main(sys.argv[1])