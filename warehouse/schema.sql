-- ============================================================
-- inDrive Rides Pipeline -- warehouse schema
-- Built ONCE by the warehouse-init service, before Airflow starts.
-- DAGs only INSERT/UPDATE into these tables -- no DDL at runtime.
-- ============================================================

-- ---------- Audit / lineage ----------
-- One row per file scripts/load.py processes, batch or streaming,
-- whether it was loaded successfully or quarantined.
CREATE SEQUENCE IF NOT EXISTS load_audit_seq START 1;
CREATE TABLE IF NOT EXISTS load_audit (
    audit_id      INTEGER PRIMARY KEY DEFAULT nextval('load_audit_seq'),
    source_type   VARCHAR NOT NULL,     -- 'batch' | 'streaming'
    file_name     VARCHAR NOT NULL,
    status        VARCHAR NOT NULL,     -- 'success' | 'failed'
    row_count     INTEGER NOT NULL DEFAULT 0,
    error_message VARCHAR,
    loaded_at     TIMESTAMP NOT NULL DEFAULT now()
);

-- ---------- Raw layer: batch referentiels ----------
-- Upserted by natural key -- a re-run with an updated CSV updates
-- existing rows and inserts new ones, instead of replacing the table.
-- source_file / loaded_at are the lineage (auditing) columns: which file
-- last touched this row, and when.

-- data-lake/input/drivers.csv
-- phone is VARCHAR, not INTEGER: Moroccan numbers start with a leading 0.
CREATE TABLE IF NOT EXISTS raw_drivers (
    driver_id   INTEGER PRIMARY KEY,
    driver_name VARCHAR,
    phone       VARCHAR,
    city        VARCHAR,
    rating      DECIMAL(3,2),           -- 0.00 .. 5.00
    joined_date DATE,
    source_file VARCHAR,
    loaded_at   TIMESTAMP
);

-- data-lake/input/vehicles.csv
-- driver_id is NOT a foreign key here on purpose: the raw layer accepts
-- whatever the source sends; referential checks happen in the transform.
CREATE TABLE IF NOT EXISTS raw_vehicles (
    vehicle_id   INTEGER PRIMARY KEY,
    driver_id    INTEGER,
    make         VARCHAR,
    model        VARCHAR,               -- VARCHAR: some models are numeric ('208')
    plate_number VARCHAR,
    vehicle_type VARCHAR,               -- 'economy' | 'comfort' | 'xl'
    source_file  VARCHAR,
    loaded_at    TIMESTAMP
);

-- data-lake/input/zones.csv
CREATE TABLE IF NOT EXISTS raw_zones (
    zone_id     INTEGER PRIMARY KEY,
    zone_name   VARCHAR,
    city        VARCHAR,
    source_file VARCHAR,
    loaded_at   TIMESTAMP
);

-- ---------- Raw layer: streaming ride requests ----------
-- Append-only: each ride request is an immutable event. ride_id is the
-- PRIMARY KEY so re-loading an already-seen file is a no-op (ON CONFLICT DO
-- NOTHING) instead of a duplicate row. processed_at is the watermark: NULL
-- means "not yet folded into fact_ride_requests" -- that's how the
-- transform avoids reprocessing the same ride request twice.
-- requested_at is UTC: one unambiguous clock for storage and SCD windows.
-- Casablanca local time is derived only in the fact table, for analysis.
CREATE TABLE IF NOT EXISTS raw_ride_requests (
    ride_id         VARCHAR PRIMARY KEY,
    requested_at    TIMESTAMP,          -- UTC
    pickup_zone_id  INTEGER,
    dropoff_zone_id INTEGER,
    status          VARCHAR,            -- 'completed' | 'cancelled' | 'no_driver'
    offered_price   DECIMAL(10,2),      -- what the passenger proposed
    final_price     DECIMAL(10,2),      -- agreed price, NULL unless completed
    distance_km     DECIMAL(6,2),       -- NULL unless completed
    driver_id       INTEGER,            -- NULL when no driver accepted
    vehicle_id      INTEGER,            -- NULL when no driver accepted
    source_file     VARCHAR,
    loaded_at       TIMESTAMP,
    processed_at    TIMESTAMP
);

-- ---------- Dimensions ----------
-- dim_date is a static calendar, not derived from any raw table -- so it is
-- seeded once, right here, at build time, and the transform never touches it.
CREATE TABLE IF NOT EXISTS dim_date (
    date_key    INTEGER PRIMARY KEY,
    full_date   DATE UNIQUE,
    day_of_week VARCHAR,
    month       INTEGER,
    year        INTEGER
);

INSERT INTO dim_date (date_key, full_date, day_of_week, month, year)
SELECT
    CAST(strftime(d, '%Y%m%d') AS INTEGER),
    d,
    strftime(d, '%A'),
    EXTRACT(month FROM d),
    EXTRACT(year FROM d)
FROM range(DATE '2026-01-01', DATE '2028-01-01', INTERVAL 1 DAY) t(d)
ON CONFLICT (date_key) DO NOTHING;

-- dim_time is also static: one row per hour of the day, to answer
-- "what time of day has the most ride requests?"
CREATE TABLE IF NOT EXISTS dim_time (
    hour        INTEGER PRIMARY KEY,    -- 0..23
    day_part    VARCHAR,                -- 'night' | 'morning' | 'afternoon' | 'evening'
    is_peak     BOOLEAN
);

INSERT INTO dim_time (hour, day_part, is_peak)
SELECT
    h,
    CASE
        WHEN h < 6  THEN 'night'
        WHEN h < 12 THEN 'morning'
        WHEN h < 18 THEN 'afternoon'
        ELSE 'evening'
    END,
    h IN (7, 8, 9, 17, 18, 19)
FROM range(0, 24) t(h)
ON CONFLICT (hour) DO NOTHING;

-- dim_zone / dim_vehicle / dim_driver are upserted from raw_* by the
-- transform tasks every run -- see scripts/transform/*.sql. Each one uses a
-- DIFFERENT SCD (Slowly Changing Dimension) strategy on purpose, so the
-- project shows all three side by side instead of picking one:

-- dim_zone -- SCD TYPE 1 (overwrite, no history). A zone being renamed
-- doesn't need old rides attributed to the old name: 1 row per zone_id.
CREATE TABLE IF NOT EXISTS dim_zone (
    zone_id   INTEGER PRIMARY KEY,
    zone_name VARCHAR,
    city      VARCHAR
);

-- dim_vehicle -- SCD TYPE 3 (previous value, limited history).
-- vehicle_type is tracked with ONE prior value alongside the current one --
-- enough to ask "which cars were upgraded from economy to comfort", not
-- enough to reconstruct a full timeline.
CREATE TABLE IF NOT EXISTS dim_vehicle (
    vehicle_id            INTEGER PRIMARY KEY,
    driver_id             INTEGER,
    make                  VARCHAR,
    model                 VARCHAR,
    plate_number          VARCHAR,
    vehicle_type          VARCHAR,
    previous_vehicle_type VARCHAR
);

-- dim_driver -- SCD TYPE 2 (full history, new row per version).
-- driver_id (natural key) can repeat across several driver_key (surrogate
-- key) rows -- one per version. valid_from/valid_to/is_current is the SCD
-- window. fact_ride_requests.sql joins on this window using requested_at,
-- so a ride is always attributed to the driver's rating and city AS THEY
-- WERE when the ride happened -- not whatever is current today.
CREATE SEQUENCE IF NOT EXISTS dim_driver_seq START 1;
CREATE TABLE IF NOT EXISTS dim_driver (
    driver_key  INTEGER PRIMARY KEY DEFAULT nextval('dim_driver_seq'),
    driver_id   INTEGER,
    driver_name VARCHAR,
    phone       VARCHAR,
    city        VARCHAR,
    rating      DECIMAL(3,2),
    joined_date DATE,
    valid_from  TIMESTAMP,              -- UTC, same clock as requested_at
    valid_to    TIMESTAMP,              -- UTC, NULL = still current
    is_current  BOOLEAN
);

-- ---------- Facts ----------
-- Grain: ONE ROW PER RIDE REQUEST (not per completed ride), so
-- "% of requests that never found a driver" is answerable.
-- Finance questions filter status = 'completed'.
-- driver_key points at the dim_driver VERSION valid at requested_at;
-- driver_id is kept too, for "all rides of driver X" across versions.
-- date_key / hour come from requested_at_local (Casablanca time), so
-- "peak hour" means the hour riders actually experienced.
CREATE TABLE IF NOT EXISTS fact_ride_requests (
    ride_id         VARCHAR PRIMARY KEY,
    date_key        INTEGER REFERENCES dim_date(date_key),
    hour            INTEGER REFERENCES dim_time(hour),
    pickup_zone_id  INTEGER REFERENCES dim_zone(zone_id),
    dropoff_zone_id INTEGER REFERENCES dim_zone(zone_id),
    driver_key      INTEGER REFERENCES dim_driver(driver_key),
    driver_id       INTEGER,
    vehicle_id      INTEGER REFERENCES dim_vehicle(vehicle_id),
    requested_at       TIMESTAMP,       -- UTC
    requested_at_local TIMESTAMP,       -- Africa/Casablanca
    status          VARCHAR,
    offered_price   DECIMAL(10,2),
    final_price     DECIMAL(10,2),
    distance_km     DECIMAL(6,2),
    price_gap       DECIMAL(10,2)       -- final_price - offered_price (the haggle)
);

-- A ride request with an unknown zone / driver / vehicle or status lands
-- here instead of fact_ride_requests -- quarantined, not silently dropped
-- or crashing the DAG.
CREATE SEQUENCE IF NOT EXISTS fact_ride_requests_rejected_seq START 1;
CREATE TABLE IF NOT EXISTS fact_ride_requests_rejected (
    reject_id       INTEGER PRIMARY KEY DEFAULT nextval('fact_ride_requests_rejected_seq'),
    ride_id         VARCHAR,
    requested_at    TIMESTAMP,          -- UTC
    pickup_zone_id  INTEGER,
    dropoff_zone_id INTEGER,
    driver_id       INTEGER,
    vehicle_id      INTEGER,
    status          VARCHAR,
    offered_price   DECIMAL(10,2),
    reason          VARCHAR,
    rejected_at     TIMESTAMP DEFAULT now()
);
