CREATE TABLE IF NOT EXISTS aircrafts (
    icao24 VARCHAR(10) PRIMARY KEY,       
    callsign VARCHAR(50),             
    origin_country VARCHAR(100),              
    category INTEGER,                         
    first_tracked_at TIMESTAMPTZ DEFAULT NOW() 
);