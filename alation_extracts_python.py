import requests
import csv
import os

# === Configuration ===
BASE_URL = "https://your-alation-domain.com"
ENDPOINTS = [
    "/integration/v1/catalog/",        # Replace with actual endpoint paths
    "/integration/v1/table/",
    # Add more endpoints as needed
]
API_TOKEN = "your_alation_api_token"
HEADERS = {
    "Authorization": f"Token {API_TOKEN}",
    "Content-Type": "application/json"
}
OUTPUT_DIR = "alation_exports"
PAGE_SIZE = 100  # Adjust as per API documentation if needed

# === Ensure output directory exists ===
os.makedirs(OUTPUT_DIR, exist_ok=True)

def fetch_all_data(endpoint):
    """
    Fetches all paginated data from a given Alation endpoint.
    Returns a list of dicts (rows).
    """
    full_url = BASE_URL + endpoint
    results = []
    offset = 0

    while True:
        params = {"limit": PAGE_SIZE, "offset": offset}
        response = requests.get(full_url, headers=HEADERS, params=params)
        
        if response.status_code != 200:
            print(f"Failed to fetch data from {endpoint}: {response.status_code}")
            break
        
        data = response.json()
        page_data = data.get("results", data if isinstance(data, list) else [])
        results.extend(page_data)

        if "next" not in data or not data["next"]:
            break

        offset += PAGE_SIZE

    return results

def save_to_csv(data, filename):
    """
    Saves a list of dictionaries to a CSV file.
    """
    if not data:
        print(f"No data to write for {filename}")
        return

    keys = sorted({key for item in data for key in item.keys()})
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)

def sanitize_filename(endpoint):
    return endpoint.strip("/").replace("/", "_")

def main():
    for endpoint in ENDPOINTS:
        print(f"Fetching data from: {endpoint}")
        data = fetch_all_data(endpoint)
        file_name = sanitize_filename(endpoint) + ".csv"
        output_path = os.path.join(OUTPUT_DIR, file_name)
        save_to_csv(data, output_path)
        print(f"Saved {len(data)} records to {output_path}")

if __name__ == "__main__":
    main()
