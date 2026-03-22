import logging
import os
import time
from core.extract import get_region_bbox, get_live_flights
from db.load import save_aircraft, save_flights_to_db, cleanup_old_data
from db.database import get_connection

# SETUP LOGGING
LOG_PATH = os.path.join("logs", "pipeline.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)


def run_pipeline():
    """
    The main execution loop.
    1. Fetches data from OpenSky.
    2. Updates the 'airplanes' registry.
    3. Saves 'live_flights' movements.
    4. Cleans up data older than 2 hours.
    """
    Ville = "France"
    bbox_info = get_region_bbox(Ville)
    if not bbox_info:
        logging.error("sadely no France boundings")
        return
    conn = get_connection()
    while True:
        try:
            logging.info("🚀 Starting new pipeline cycle...")
            
            # 1. EXTRACT
            raw_data = get_live_flights(bbox_info)
            if not raw_data:
                continue
            
            # 2. LOAD DIMENSION (Airplanes)
            # ON CONFLICT DO NOTHING handles the duplicates
            save_aircraft(raw_data, conn)

            # 3. LOAD FACT (Movements)
            save_flights_to_db(raw_data, conn)
            
            # 4. MANAGE STORAGE (The 'Anti-Explosion' step)
            cleanup_old_data(conn, hours=1)
            
            conn.commit()
            logging.info("😴 Cycle complete. Sleeping for 30s...")
            time.sleep(30) # OpenSky public API limit is roughly 1 min
            
            
        except Exception as e:
            logging.error(f"🚨 Pipeline crashed: {e}")
            time.sleep(10) # Wait before retrying

if __name__ == "__main__":
    run_pipeline()