import requests

def get_api_data(url):
    print(f"Pulling data from {url}", flush=True)
    data = requests.get(url)
    
    # Convert to JSON format
    return scrub_data(data.json())

def scrub_data(data):
    pm_dict = data["sensors"][0]["data"]["PM2.5"]
    relevant_data = [f"{x['Timestamp']}:{x['Value']}" for x in pm_dict]
    return relevant_data