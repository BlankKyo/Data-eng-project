# app/database.py
import os
from dotenv import load_dotenv
import psycopg2

load_dotenv(".env.app")  # Load environment variables from .env.app

def get_connection():
    return psycopg2.connect(
        host = os.getenv("DB_HOST", "db"),
        database = os.getenv("DB_NAME"),
        user = os.getenv("DB_USER"),
        password = os.getenv("DB_PASS")
    )

def init_db():
    """Run this once at start-up to ensure tables exist."""
    conn = get_connection()
    query = """
    CREATE TABLE IF NOT EXISTS live_flights (
        observed_at TIMESTAMPTZ NOT NULL,
        flight_icao VARCHAR(10),
        airline VARCHAR(100),
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        altitude REAL,
        horizontal_speed REAL,
        status VARCHAR(20)
    );
    """
    with conn.cursor() as cur:
        cur.execute(query)
        conn.commit()
    conn.close()
    print("✅ Database initialized.")