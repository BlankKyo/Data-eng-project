# app/database.py
import logging
import os
from dotenv import load_dotenv
import psycopg2

# Load environment variables from .env.app
logger = logging.getLogger(__name__)

def get_connection():
    logger.info("Establishing database connection...")
    return psycopg2.connect(
        host = os.getenv("DB_HOST", "db"),
        database = os.getenv("DB_NAME"),
        user = os.getenv("DB_USER"),
        password = os.getenv("DB_PASS")
    )

def create_table(filename, conn):
    """
    Reads a SQL script from the local filesystem and executes it.

    This helper centralizes SQL execution to keep the main logic clean. 
    It assumes SQL files are stored in a sub-directory named 'queries' 
    relative to this script.

    Args:
        filename (str): The name of the .sql file (e.g., 'airplanes.sql').
        conn (psycopg2.extensions.connection): An active database connection object.

    Raises:
        FileNotFoundError: If the specified SQL file does not exist in /queries.
        psycopg2.Error: If the SQL syntax is invalid or a database constraint is violated.
        IOError: If there is an issue reading the file from the disk.
    """
    query_path = os.path.join(os.path.dirname(__file__), "queries", filename)
    
    if not os.path.exists(query_path):
        raise FileNotFoundError(f"SQL file not found: {query_path}")

    try:
        with open(query_path, "r") as f:
            sql = f.read()
            
        with conn.cursor() as cur:
            cur.execute(sql)
        logger.info(f"Successfully read and executed {filename}")
        
    except psycopg2.Error as e:
        logger.error(f"Database error while running {filename}: {e}")
        raise 


def init_db():
    """
    Sets up the database tables using SQL files.

    This function connects to the DB and runs 'airplanes.sql' then 
    'live_flights.sql'. It is safe to run every time the app starts.

    Note:
        - If one file fails, neither table is created (Rollback).
        - Always closes the connection, even if it crashes.

    Raises:
        Exception: If the database is down or SQL files are missing.
    """
    conn = None
    try:
        conn = get_connection()

        create_table("airplanes.sql", conn)
        create_table("live_flights.sql", conn)
        
        conn.commit()
        logger.info("Database initialized successfully.")
        
    except Exception as e:
        if conn:
            conn.rollback()
            logger.warning("Transaction rolled back due to error.")
        logger.critical(f"Failed to initialize database: {e}")
        
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")