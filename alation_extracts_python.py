import requests
import csv
import os
import pandas as pd

# === Configuration ===
BASE_URL = ""
ENDPOINTS = [
    "/integration/v1/query/",        # Replace with actual endpoint paths
    "/integration/v2/domain/",
    "/integration/v1/agent/",
    "/integration/v2/bi/server/",
    "/integration/v2/bi/server/1/Apps& Workspaces/",
    "/integration/v2/bi/server/1/connection/",
    "/integration/v2/bi/server/1/connection/column/",
    "/integration/v2/bi/server/1/report/",
    "/integration/v2/bi/server/1/report/column/",
    "/integration/v2/bi/server/1/datasource/",
    "/integration/v2/bi/server/1/datasource/column/",
    "/integration/v2/bi/server/1/permission/",
    "/integration//v2/bi/server/1/user/",
    "/integration/v2/connectors/",
    "/integration/v1/conversations/",
    "/integration/v2/conversations/",
    "/integration/v2/custom_field/",
    "/integration/v2/custom_field_value/",
    "/integration/v1/data_quality/fields/",
    "/integration/v1/data_quality/values/",
    "/integration/v1/datasource/",
    "/integration/v2/datasource/",
    "/integration/v2/document/",
    "/integration/v2/folder/",
    "/integration/v1/group/",
    "/integration/v2/dataflow/",
    "/integration/v2/lineage/",
    "/integration/v2/cross_system_lineage/",
    "/integration/v2/doc_schema/56/",
    "/integration/v1/otype/",
    "/integration/v1/business_policies/",
    "/integration/v1/policy_group/",
    "/integration/v2/schema/",
    "/integration/v2/table/",
    "/integration/v2/column/",
    "/integration/v1/search/",
    "/integration/v1/search_synonym/",
    # "/integration/v2/term/?limit=1000",
    "/integration/v1/user",
    "/integration/v1/generate_dup_users_accts_csv_file/",
    "/integration/v2/user/",
    "/integration/v2/workflows/",
    "/integration/v2/workflow_executions/",
    "/integration/v1/custom_template/",
    "/integration/flag/",
    "/integration/tag/"
]
API_TOKEN = ""
HEADERS = {
    "Token": API_TOKEN,
    "Content-Type": "application/json"
}
OUTPUT_DIR = "alation_exports"
PAGE_SIZE = 1000  # Adjust as per API documentation if needed

# === Ensure output directory exists ===
os.makedirs(OUTPUT_DIR, exist_ok=True)

def fetch_all_data(endpoint):
    """
    Fetches all paginated data from a given Alation endpoint.
    Returns a list of dicts (rows).
    """
    full_url = BASE_URL + endpoint
    print(full_url)
    results = []
    offset = 0

    while True:
        params = {"offset": offset, "limit": PAGE_SIZE}
        response = requests.get(full_url, headers=HEADERS, params=params)

        if response.status_code != 200:
            print(f"Failed to fetch: {response.status_code} - {response.text}")
            break

        data = response.json()
        if not data:
            break

        # Handle nested format (e.g. if response = {"results": [...]})
        if isinstance(data, dict) and "results" in data:
            page_data = data["results"]
        elif isinstance(data, list):
            page_data = data
        else:
            print(f"Unexpected response format: {type(data)}")
            break

        if not page_data:
            break

        results.extend(page_data)
        print(f"Fetched {len(page_data)} records (offset={offset})")

        if len(page_data) < PAGE_SIZE:
            break  # Last page

        offset += PAGE_SIZE

    return pd.json_normalize(results)

def save_to_csv(data, filename):
    """
    Saves a list of dictionaries to a CSV file.
    """
    print("Inside saving a file")
    if data is not None and len(data) > 0:
        data.to_csv(f"{filename}", index=False)
        return len(data)
    else :
        print(f"No data to write for {filename}")
        return 0

def sanitize_filename(endpoint):
    return endpoint.strip("/").replace("/", "_")

def main():
    for endpoint in ENDPOINTS:
        print(f"Fetching data from: {endpoint}")
        data = fetch_all_data(endpoint)
        print(data)
        print("data fetched")
        file_name = sanitize_filename(endpoint) + ".csv"
        print("File name set")
        output_path = os.path.join(OUTPUT_DIR, file_name)
        print("output path saved")
        len_data = 0
        len_data = save_to_csv(data, output_path)
        print(f"Saved {len_data} records to {output_path}")

if __name__ == "__main__":
    main()
