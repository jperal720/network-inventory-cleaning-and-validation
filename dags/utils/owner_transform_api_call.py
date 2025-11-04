import requests

def call_process_file(csv_path: str):
    '''Makes an API call to our deepseek-powered fastapi server.
    
    Returns -> csv_name (str) or None'''
    URL = "http://transformation-server:8000/process-file"
    PARAMS = {"return_csv": "false"}
    FILES = {
        "file": (
            f"{csv_path.split('/')[-1]}",
            open(f"{csv_path}", "rb"),
            "text/csv"
        )
    }
    HEADERS = {
        "accept": "application/json"
    }

    response = requests.post(URL, params=PARAMS, files=FILES, headers=HEADERS)

    if response.status_code == 200:
        data = response.json()
        csv_name = data["updated_csv_path"].split('/')[-1]
        print(f"Success:  {csv_name} is found in tmp.")
        return csv_name
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        return None