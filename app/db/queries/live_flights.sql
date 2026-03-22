CREATE TABLE IF NOT EXISTS live_flights (
        id SERIAL PRIMARY KEY,
        flight_icao VARCHAR(10) REFERENCES aircraftS(icao24),
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        baro_altitude REAL,
        geo_altitude REAL,
        velocity REAL,
        vertical_rate REAL,
        observed_at TIMESTAMPTZ NOT NULL
    );