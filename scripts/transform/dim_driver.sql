-- Upsert dim_driver from raw_drivers by natural key (driver_id).
-- SCD TYPE 2: a change to any tracked attribute closes the current row
-- (valid_to = now, is_current = false) and opens a brand new one instead of
-- overwriting -- full history, one row per version. fact_ride_requests.sql
-- joins on [valid_from, valid_to) using the ride's own requested_at, so a
-- ride is always attributed to the driver's rating/city at that moment.
--
-- "now" is timezone('UTC', now()): UTC, the same clock requested_at is
-- stored in, whatever the DuckDB session's TimeZone happens to be.
-- One transaction: now() is the transaction start time, so step 1's
-- valid_to and step 2's valid_from are exactly the same instant -- no gap
-- and no overlap between two versions of the same driver.
BEGIN TRANSACTION;

-- step 1: close out any current version whose tracked attributes changed
UPDATE dim_driver
SET valid_to = timezone('UTC', now()), is_current = false
WHERE is_current = true
  AND driver_id IN (
      SELECT r.driver_id
      FROM raw_drivers r
      JOIN dim_driver d ON d.driver_id = r.driver_id AND d.is_current = true
      WHERE d.driver_name IS DISTINCT FROM r.driver_name
         OR d.phone       IS DISTINCT FROM r.phone
         OR d.city        IS DISTINCT FROM r.city
         OR d.rating      IS DISTINCT FROM r.rating
         OR d.joined_date IS DISTINCT FROM r.joined_date
  );

-- step 2: open a new current version for anything without one -- brand-new
-- driver_ids, and any driver_id just closed above (both have no is_current
-- row left after step 1, so one WHERE NOT EXISTS covers both cases).
-- A driver's FIRST version is valid from joined_date, not from now: rides
-- streamed before this DAG first ran must still find a matching version.
INSERT INTO dim_driver (driver_id, driver_name, phone, city, rating, joined_date, valid_from, valid_to, is_current)
SELECT
    r.driver_id, r.driver_name, r.phone, r.city, r.rating, r.joined_date,
    CASE
        WHEN EXISTS (SELECT 1 FROM dim_driver d WHERE d.driver_id = r.driver_id)
            THEN timezone('UTC', now())
        ELSE CAST(r.joined_date AS TIMESTAMP)
    END,
    NULL,
    true
FROM raw_drivers r
WHERE NOT EXISTS (
    SELECT 1 FROM dim_driver d WHERE d.driver_id = r.driver_id AND d.is_current = true
);

COMMIT;
