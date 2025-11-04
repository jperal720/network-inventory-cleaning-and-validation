import requests

def call_anomaly_detector(csv_path: str):

    URL = "http://transformation-server:8000/detect-anomalies"
    PARAMS = {}
    FILES = {
        "file": (
            f"{csv_path.split('/')[-1]}",
            open(f"{csv_path}", "rb"),
            "text/csv"
        )
    }
    HEADERS= {
        "accept": "application/json"
    }

    response = requests.post(URL, params=PARAMS, files=FILES, headers=HEADERS)

    if response.status_code == 200:
        data = response.json()
        json_name = data["anomaly_response_path"].split("/")[-1]
        print(f"Success: {json_name} is found in tmp")
        return json_name
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        return None