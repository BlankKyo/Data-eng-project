import requests
import logging
import os
import json
from datetime import datetime

# SETUP LOGGING
LOG_PATH = os.path.join("..", "logs", "pipeline.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)

def fetch_repo_data():
    """
    Fetches metadata for a specific GitHub repository via the GitHub REST API.

    This function performs an authenticated or unauthenticated GET request to 
    the GitHub API. It handles network timeouts and common HTTP errors to 
    ensure pipeline stability.

    Args:
        repo_owner (str): The GitHub username or organization (e.g., 'google').
        repo_name (str): The name of the repository (e.g., 'go').

    Returns:
        Optional[Dict[str, Any]]: A dictionary containing the repository metadata 
            if successful; None if an error occurs during extraction.

    Raises:
        requests.exceptions.HTTPError: If the API returns a non-200 status code.
        requests.exceptions.Timeout: If the server takes longer than 10 seconds to respond.
        requests.exceptions.RequestException: For any other network-related issues.
    """
    api_url = "https://api.github.com/orgs/google/repos?per_page=10&sort=updated"
    
    try:
        logging.info("Connecting to GitHub API to fetch Google's latest repos...")
        
        # Adding a User-Agent is a best practice to avoid 403 errors
        headers = {'User-Agent': 'Data-Intern-Project'}
        response = requests.get(api_url, headers=headers, timeout=10)
        
        # The 'Senior' way to handle the response
        if response.status_code == 200:
            logging.info("success: 200 OK. Data retrieved.")
            return {"repos": response.json()}
        else:
            logging.error(f"Failed with status code: {response.status_code}")
            return None

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None
    

Data_PATH = os.path.join("..", "db", "raw")

def save_raw_data(data, folder=Data_PATH):
    """
    Saves the raw JSON data to a local file system with a unique timestamp.

    This function ensures the destination directory exists, generates a 
    filename based on the current system time, and persists the dictionary 
    as a formatted JSON file.

    Args:
        data (dict): The dictionary containing API response data to be saved.
        folder (str, optional): The relative path to the target directory. 
            Defaults to "data/raw".

    Returns:
        None

    Raises:
        OSError: If the directory cannot be created or the file cannot be written.
        TypeError: If the provided data is not JSON serializable.
    """
    if not data:
        return
    
    os.makedirs(folder, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{folder}/repo_data_{timestamp}.json"
    
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)
    
    logging.info(f"Raw data saved to {filename}")

if __name__ == "__main__":
    raw_json = fetch_repo_data()
    save_raw_data(raw_json)