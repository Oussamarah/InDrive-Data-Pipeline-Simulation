-- Upsert dim_zone from raw_zones by natural key (zone_id).
-- SCD TYPE 1: plain overwrite, no history kept -- see schema.sql for why.
INSERT INTO dim_zone (zone_id, zone_name, city)
SELECT zone_id, zone_name, city
FROM raw_zones
ON CONFLICT (zone_id) DO UPDATE SET
    zone_name = EXCLUDED.zone_name,
    city      = EXCLUDED.city;
