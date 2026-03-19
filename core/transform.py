import logging
import pandas as pd

def transform_flight_data(raw_flights):
    """
    Transforms raw OpenSky state vectors into a structured Silver-layer DataFrame.
    
    Mapping the raw list indices to official OpenSky column names and 
    cleaning flight identifiers.

    Args:
        raw_flights (list): The 'states' list from the OpenSky API response.

    Returns:
        pd.DataFrame: A cleaned DataFrame. Returns an empty DF if input is empty.
    """
    logging.info(f"Starting transformation for {len(raw_flights)} raw records.")

    if not raw_flights:
        logging.warning("No flight data received. Returning empty DataFrame.")
        return pd.DataFrame()

    try:
        # 1. Define the OpenSky state vector mapping (Positions 0-16)
        # Reference: https://openskynetwork.github.io/opensky-api/rest.html#response
        columns = [
            "icao24", "callsign", "origin_country", "time_position", 
            "last_contact", "longitude", "latitude", "baro_altitude", 
            "on_ground", "velocity", "true_track", "vertical_rate", 
            "sensors", "geo_altitude", "squawk", "spi", "position_source"
        ]

        # 2. Create the DataFrame
        df = pd.DataFrame(raw_flights, columns=columns)

        # 3. SILVER LAYER CLEANING
        # - Remove trailing spaces from callsigns (OpenSky often pads them)
        df['callsign'] = df['callsign'].str.strip()
        
        # - Drop rows where we don't even have a location (useless for mapping)
        df = df.dropna(subset=['longitude', 'latitude'])

        # - Select only the columns we need for the next phase
        essential_cols = [
            "icao24", "callsign", "origin_country", 
            "longitude", "latitude", "velocity", "baro_altitude"
        ]
        
        df_silver = df[essential_cols].copy()
        
        logging.info(f"Transformation complete. {len(df_silver)} valid flights processed.")
        return df_silver

    except Exception as e:
        logging.error(f"Failed to transform flight data: {e}")
        return pd.DataFrame()