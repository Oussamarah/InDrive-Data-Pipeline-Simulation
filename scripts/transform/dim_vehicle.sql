-- Upsert dim_vehicle from raw_vehicles by natural key (vehicle_id).
-- SCD TYPE 3: when vehicle_type changes (e.g. economy -> comfort), keep the
-- ONE prior value in previous_vehicle_type and let vehicle_type move on.
-- Re-running with an unchanged vehicle_type is a no-op --
-- previous_vehicle_type only moves when the incoming value actually differs.
-- A second change overwrites the first prior value: that's Type 3's limit.
INSERT INTO dim_vehicle (vehicle_id, driver_id, make, model, plate_number, vehicle_type, previous_vehicle_type)
SELECT vehicle_id, driver_id, make, model, plate_number, vehicle_type, NULL
FROM raw_vehicles
ON CONFLICT (vehicle_id) DO UPDATE SET
    driver_id             = EXCLUDED.driver_id,
    make                  = EXCLUDED.make,
    model                 = EXCLUDED.model,
    plate_number          = EXCLUDED.plate_number,
    previous_vehicle_type = CASE
        WHEN dim_vehicle.vehicle_type IS DISTINCT FROM EXCLUDED.vehicle_type THEN dim_vehicle.vehicle_type
        ELSE dim_vehicle.previous_vehicle_type
    END,
    vehicle_type          = EXCLUDED.vehicle_type;
