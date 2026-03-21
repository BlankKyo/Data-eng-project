from db.database import get_connection
from psycopg2.extras import execute_batch
import logging

logger = logging.getLogger(__name__)

def save_flights_to_db(flight_list):
    """Inserts clean records into the live_flights table."""
    conn = get_connection()
    query = """
    INSERT INTO live_flights (observed_at, flight_icao, latitude, longitude, altitude, horizontal_speed)
    VALUES (%(observed_at)s, %(icao)s, %(lat)s, %(lon)s, %(alt)s, %(speed)s);
    """
    with conn.cursor() as cur:
        # execute_batch is much faster for 'Production' than a loop!
        
        execute_batch(cur, query, flight_list)
        conn.commit()
    conn.close()
    logger.info(f"Successfully loaded {len(flight_list)} flights to DB.")