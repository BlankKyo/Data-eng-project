import requests
import logging
import os
from datetime import datetime

# This automatically names the logger after the file (e.g., core.extract)
logger = logging.getLogger(__name__)

def get_region_bbox(place_name):
    """
    Fetches the geographical bounding box for a location via Nominatim API.

    Queries the API, saves the raw result to the bronze data layer, and 
    extracts the min/max latitude and longitude.

    Args:
        place_name (str): The search query (e.g., 'Berlin' or 'Paris').

    Returns:
        list: [dict of coordinates, place_name] or None if not found.
    """
    logger.info(f"Starting bounding box extraction for: {place_name}")
    try:

        url = os.getenv("NOMINATIM_URL")
        params = {'q': place_name, 'format': 'json', 'limit': 1}
        headers = {'User-Agent': 'FlightTrackerInternApp/1.0'} # Required by Nominatim
        
        response = requests.get(url, params=params, headers=headers)
        data = response.json()
        
        
        if data:
            bbox = data[0]['boundingbox']
            return [{
                'lamin': float(bbox[0]),
                'lamax': float(bbox[1]),
                'lomin': float(bbox[2]),
                'lomax': float(bbox[3])
            }, place_name]
        return None
    except Exception as e:
        logger.error(f"Error fetching bounding box for {place_name}: {e}")
        return None

def get_live_flights(bbox):
    """
    Fetch real-time flight data from OpenSky Network within a bounding box.

    Connects to the OpenSky API, saves the raw response to the bronze layer,
    and extracts the state vectors (flights) found in the specified region.

    Args:
        bbox (list): A list containing [dict of coordinates, place_name].
                    The dict must have 'lamin', 'lamax', 'lomin', 'lomax' keys.

    Returns:
        list: A list of state vectors (lists) representing flights. 
              Returns an empty list if no flights are found or if the request fails.
    """
    url = os.getenv("OPENSKY_URL")
    logger.info(f"Starting live airplanes extraction for: {bbox[1]}")
    try:
        response = requests.get(url, params=bbox[0])  
        
        if response.status_code == 200:
            data = response.json()
            # Extract just the 'states' list
            flights = data.get('states', []) or []
            raw_time = data.get('time', datetime.now().timestamp())
            formatted_time = datetime.fromtimestamp(raw_time).strftime('%Y%m%d%H%M%S')
            logger.info(f"Extracted {len(flights)} flights at {formatted_time}")
            return flights
        else:
            logger.error(f"Error: API returned status code {response.status_code}")
            return []
            
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return []
