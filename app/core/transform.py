# core/transform.py
from datetime import timezone, datetime
import logging

# init logger
logger = logging.getLogger(__name__)

def aircrafts_data(raw_flights):
    """
    Extracts aircraft identity data from raw OpenSky vectors.

    This function filters the 17-item OpenSky state list into a format 
    suitable for the 'airplanes' table. It handles timestamp conversion 
    and basic string cleaning.

    Args:
        raw_flights (list): The 'states' list from the OpenSky API response.

    Returns:
        list: A list of dictionaries, each representing one unique aircraft.
              Returns an empty list if no data is provided or an error occurs.
    """
    logger.info(f"Starting transformation for {len(raw_flights)} raw records.")

    if not raw_flights:
        logger.warning("No flight data received. Returning empty DataFrame.")
        return []

    try:
        clean_data = []
        for state in raw_flights:
            clean_data.append({
                'icao24': state[0],             
                'callsign': state[1].strip() if state[1] else None, 
                'origin_country': state[2].strip(),      
                'category': state[17] if len(state) > 17 else 0,                  
                'first_tracked_at': datetime.fromtimestamp(state[3] or state[4], tz=timezone.utc)         
            })
        return clean_data

    except Exception as e:
        logger.error(f"Failed to transform flight data: {e}")
        return []
    

def live_flights_data(raw_flights):
    """
    Extracts real-time movement data from raw OpenSky vectors.

    This function filters for airborne aircraft with valid GPS coordinates 
    and maps movement indices (velocity, altitude, etc.) to a format 
    compatible with the 'live_flights' table.

    Args:
        raw_flights (list): The 'states' list from the OpenSky API response.

    Returns:
        list: A list of dictionaries with geographic and movement data.
              Filters out records with missing coordinates or planes on the ground.
    """
    if not raw_flights:
        logger.warning("No flight data received.")
        return []

    clean_data = []
    try:
        for state in raw_flights:
            # We skip planes that don't have a location (null lat/long)
            # because we can't track them over Pessac without coordinates!
            if state[5] is None or state[6] is None or state[8]:
                continue

            clean_data.append({
                'flight_icao': state[0],
                'latitude': state[6],            # Index 6: Latitude
                'longitude': state[5],           # Index 5: Longitude
                'baro_altitude': state[7],       # Index 7: Baro Altitude
                'geo_altitude': state[13],       # Index 13: Geometric Altitude
                'velocity': state[9],            # Index 9: Velocity (m/s)
                'vertical_rate': state[11],      # Index 11: Vertical Rate (m/s)
                # Convert Unix timestamp (state[3] or state[4]) to Python datetime
                'observed_at': datetime.fromtimestamp(state[3] or state[4], tz=timezone.utc)
            })
        
        logger.info(f"Transformed {len(clean_data)} live flight records.")
        return clean_data

    except Exception as e:
        logger.error(f"Transformation error: {e}")
        return []