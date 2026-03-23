# db/load.py
import psycopg2
from psycopg2.extras import execute_batch
from core.transform import aircrafts_data, live_flights_data
import logging

logger = logging.getLogger(__name__)

def save_aircraft(raw_data_list, conn):
    """
    Filters and loads unique aircraft records into the airplanes table.

    Processes a list of raw state vectors, extracts specific columns 
    (Indices: 0, 1, 2, etc.), and performs a batch insert. Skips 
    duplicates using the 'ON CONFLICT' rule.

    Args:
        raw_data_list (list): A list of lists representing OpenSky state vectors.
        conn: A database connection object.
    """
    clean_data = aircrafts_data(raw_data_list)
    if not clean_data:
        logger.warning("No aircraft data provided to save.")
        return

    query = """
    INSERT INTO aircrafts (icao24, callsign, origin_country, category, first_tracked_at)
    VALUES (%(icao24)s, %(callsign)s, %(origin_country)s, %(category)s, %(first_tracked_at)s)
    ON CONFLICT (icao24) DO NOTHING;
    """

    try:
        with conn.cursor() as cur:
            # Batch execution is 10x faster for 1,000+ rows
            execute_batch(cur, query, clean_data)
        logger.info(f"Batch load complete: {len(clean_data)} vectors processed.")
    except psycopg2.Error as e:
        logger.error(f"Database error during aircraft save: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in save_aircraft: {e}")
        raise




def save_flights_to_db(flight_list, conn):
    """
    Saves real-time flight movement data to the database.

    Transforms raw flight vectors and performs a batch insert into the 
    'live_flights' table. Each record represents a snapshot in time.

    Args:
        flight_list (list): Raw OpenSky state vectors.
        conn: A database connection object.
    """
    clean_data = live_flights_data(flight_list)
    if not clean_data:
        logger.warning("No valid live flight data to save.")
        return

    query = """
    INSERT INTO live_flights (
        flight_icao, latitude, longitude, baro_altitude, 
        geo_altitude, velocity, vertical_rate, observed_at
    )
    VALUES (
        %(flight_icao)s, %(latitude)s, %(longitude)s, %(baro_altitude)s, 
        %(geo_altitude)s, %(velocity)s, %(vertical_rate)s, %(observed_at)s
    );
    """

    try:
        with conn.cursor() as cur:
            # Batch execution handles thousands of pings efficiently
            execute_batch(cur, query, clean_data)
        logger.info(f"Successfully logged {len(clean_data)} flight positions.")

    except psycopg2.Error as e:
        logger.error(f"DB error while saving live flights: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in save_flights_to_db: {e}")
        raise


def cleanup_old_data(conn, hours=2):
    """
    Deletes flight positions older than a specific timeframe to save disk space.
    
    This keeps the 'live_flights' table lean. We keep the 'airplanes' table 
    intact as it is a small dimension table.
    """
    # SQL query using PostgreSQL's INTERVAL feature
    query = "DELETE FROM live_flights WHERE observed_at < NOW() - INTERVAL '%s hours';"
    
    try:
        with conn.cursor() as cur:
            cur.execute(query, (hours,))
            rows_deleted = cur.rowcount
        
        if rows_deleted > 0:
            logger.info(f"Database Maintenance: Purged {rows_deleted} old flight pings.")
        else:
            logger.debug("Database Maintenance: No old data to purge yet.")

    except psycopg2.Error as e:
        logger.error(f"Cleanup failed: {e}")
    except Exception as e:
        logger.error(f"Unexpected error during cleanup: {e}")