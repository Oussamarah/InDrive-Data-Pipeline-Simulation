-- Fold every not-yet-processed ride request (raw_ride_requests.processed_at
-- IS NULL) into fact_ride_requests / fact_ride_requests_rejected, then set
-- the watermark so this run's rides are never picked up again.
-- Runs after dim_zone / dim_vehicle / dim_driver so the joins below see
-- fresh dimension data. One transaction: either the whole batch lands and
-- is watermarked, or none of it does.
BEGIN TRANSACTION;

-- requested_at is stored in UTC; date_key and hour are taken from Casablanca
-- local time. timezone('UTC', ts) reads the naive timestamp as UTC, then
-- timezone('Africa/Casablanca', ...) renders it as local wall-clock time --
-- independent of the session's TimeZone setting.
CREATE OR REPLACE TEMP TABLE _new_rides AS
SELECT *, timezone('Africa/Casablanca', timezone('UTC', requested_at)) AS requested_at_local
FROM raw_ride_requests WHERE processed_at IS NULL;

-- Resolve every foreign key once, then decide accept/reject from the result.
-- RULE 2: pick the dim_driver VERSION that was valid when the ride was
-- requested (requested_at inside its [valid_from, valid_to) SCD window) --
-- not just "whatever the driver's rating is today".
CREATE OR REPLACE TEMP TABLE _classified AS
SELECT
    r.*,
    CAST(strftime(r.requested_at_local, '%Y%m%d') AS INTEGER) AS date_key,
    CAST(EXTRACT(hour FROM r.requested_at_local) AS INTEGER)  AS hour,
    d.driver_key,
    -- RULE 1: first failing check wins; NULL reason = accepted
    CASE
        WHEN r.status NOT IN ('completed', 'cancelled', 'no_driver') OR r.status IS NULL
            THEN 'unknown status'
        WHEN dd.date_key IS NULL     THEN 'requested_at outside dim_date'
        WHEN pz.zone_id IS NULL      THEN 'unknown pickup_zone_id'
        WHEN dz.zone_id IS NULL      THEN 'unknown dropoff_zone_id'
        WHEN r.driver_id IS NOT NULL AND d.driver_key IS NULL
            THEN 'unknown driver_id (no version valid at requested_at)'
        WHEN r.vehicle_id IS NOT NULL AND v.vehicle_id IS NULL
            THEN 'unknown vehicle_id'
        WHEN r.status = 'completed' AND (r.driver_id IS NULL OR r.final_price IS NULL)
            THEN 'completed ride without driver or final_price'
    END AS reason
FROM _new_rides r
LEFT JOIN dim_date dd ON dd.date_key = CAST(strftime(r.requested_at_local, '%Y%m%d') AS INTEGER)
LEFT JOIN dim_zone pz ON pz.zone_id = r.pickup_zone_id
LEFT JOIN dim_zone dz ON dz.zone_id = r.dropoff_zone_id
LEFT JOIN dim_vehicle v ON v.vehicle_id = r.vehicle_id
LEFT JOIN dim_driver d
    ON d.driver_id = r.driver_id
    AND r.requested_at >= d.valid_from
    AND (d.valid_to IS NULL OR r.requested_at < d.valid_to);

-- a ride with a bad reference is quarantined here instead of silently
-- dropped or crashing the DAG
INSERT INTO fact_ride_requests_rejected
    (ride_id, requested_at, pickup_zone_id, dropoff_zone_id, driver_id, vehicle_id, status, offered_price, reason)
SELECT ride_id, requested_at, pickup_zone_id, dropoff_zone_id, driver_id, vehicle_id, status, offered_price, reason
FROM _classified
WHERE reason IS NOT NULL;

INSERT INTO fact_ride_requests
    (ride_id, date_key, hour, pickup_zone_id, dropoff_zone_id, driver_key, driver_id, vehicle_id,
     requested_at, requested_at_local, status, offered_price, final_price, distance_km, price_gap)
SELECT
    ride_id, date_key, hour, pickup_zone_id, dropoff_zone_id, driver_key, driver_id, vehicle_id,
    requested_at, requested_at_local, status, offered_price, final_price, distance_km,
    final_price - offered_price AS price_gap
FROM _classified
WHERE reason IS NULL
ON CONFLICT (ride_id) DO NOTHING;

-- RULE 0: watermark exactly the rides read above, not "everything NULL now"
UPDATE raw_ride_requests SET processed_at = now()
WHERE ride_id IN (SELECT ride_id FROM _new_rides);

COMMIT;
