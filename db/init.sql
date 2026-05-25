CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS processed;

CREATE TABLE raw.raw_weather(
    id          BIGINT GENERATED ALWAYS AS IDENTITY,
    source      TEXT NOT NULL,
    payload     JSONB,                             
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (id)    
);

CREATE TABLE raw.raw_aqi(
    id          BIGINT GENERATED ALWAYS AS IDENTITY,
    source      TEXT NOT NULL,         
    payload     JSONB,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (id)
);

CREATE TABLE processed.hourly_metrics(
    id              BIGINT GENERATED ALWAYS AS IDENTITY,
    city_code       VARCHAR(10),
    timestamp_local TIMESTAMPTZ,           
    temperature_c   DECIMAL(4,1),          
    humidity_pct    NUMERIC(5,2),           
    windspeed_kmh   NUMERIC(5,2),           
    pm2_5           DECIMAL(6,2),           
    pm10            DECIMAL(6,2),           
    aqi_eu          INT,           
    health_risk     VARCHAR(20) CHECK (health_risk IN 
                    ('Good','Moderate','Unhealthy','Hazardous')),
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (id),
    UNIQUE (timestamp_local,city_code)                   
);

CREATE TABLE processed.forecast_alerts(
    id              BIGINT GENERATED ALWAYS AS IDENTITY,
    city_code       VARCHAR(10),
    city_name       VARCHAR(50),
    timestamp_local TIMESTAMPTZ,
    health_risk     VARCHAR(20),
    aqi_value       INT,
    message         TEXT,
    acknowledged    BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    forecast_for_time TIMESTAMPTZ,
    lead_time_hours INT,
    PRIMARY KEY (id)
);
