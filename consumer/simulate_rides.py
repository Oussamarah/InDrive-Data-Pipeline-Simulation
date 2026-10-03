"""Streaming source: simulates one inDrive ride request every second and
drops it as a JSON file into data-lake/input/streaming/.

inDrive's twist on ride hailing is that the PASSENGER proposes the price
(offered_price) and drivers accept it or counter-offer -- so final_price can
differ from offered_price, and a lowball offer is more likely to end with no
driver at all. That's what fact_ride_requests.price_gap and the
'no_driver' status let us analyse.

Occasionally injects a bad zone_id / driver_id on purpose, so the transform
has something real to quarantine in fact_ride_requests_rejected."""
import json
import math
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone

OUT_DIR = "/app/raw_rides"
PULL_INTERVAL_SECONDS = float(os.environ.get("PULL_INTERVAL_SECONDS", "1"))
BAD_REFERENCE_RATE = 0.05  # 5% of ride requests reference a zone/driver that doesn't exist
BAD_ID = 999

# matches data-lake/input/batch/zones.csv -- approximate centre of each zone,
# only used to make distances (and therefore prices) look plausible
ZONE_COORDS = {
    1: (33.5847, -7.6324),  # Maarif
    2: (33.5928, -7.6858),  # Ain Diab
    3: (33.5990, -7.6150),  # Sidi Belyout
    4: (33.5600, -7.6650),  # Hay Hassani
    5: (33.5960, -7.6400),  # Bourgogne
    6: (33.5500, -7.6800),  # Oulfa
    7: (33.5800, -7.5100),  # Sidi Moumen
    8: (33.5880, -7.6600),  # Anfa
}
# matches data-lake/input/batch/{drivers,vehicles}.csv -- driver_id -> vehicle_id
DRIVER_VEHICLE = {d: d for d in range(1, 9)}

BASE_FARE_MAD = 7.0
PER_KM_MAD = 3.5
MIN_FARE_MAD = 15.0


def road_distance_km(pickup, dropoff):
    """Straight-line distance between zone centres x 1.3 for real roads, plus
    some jitter since pickup/dropoff points aren't exactly the centre."""
    (lat1, lon1), (lat2, lon2) = ZONE_COORDS[pickup], ZONE_COORDS[dropoff]
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    straight = 6371 * 2 * math.asin(math.sqrt(a))
    return round(max(1.0, straight * 1.3 + random.uniform(0.5, 2.5)), 2)


def build_ride_request():
    # naive UTC on purpose: the warehouse stores UTC and converts to
    # Casablanca time itself, so only ONE time zone database is involved
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    pickup, dropoff = random.choice(list(ZONE_COORDS)), random.choice(list(ZONE_COORDS))
    distance_km = road_distance_km(pickup, dropoff)
    fair_price = max(MIN_FARE_MAD, BASE_FARE_MAD + PER_KM_MAD * distance_km)

    # the passenger names their price: sometimes generous, often a lowball
    offered_price = round(fair_price * random.uniform(0.75, 1.15))
    lowball = offered_price < fair_price * 0.85

    roll = random.random()
    if roll < (0.40 if lowball else 0.08):
        status = "no_driver"
    elif roll < (0.50 if lowball else 0.18):
        status = "cancelled"
    else:
        status = "completed"

    driver_id = vehicle_id = final_price = None
    if status != "no_driver":
        driver_id = random.choice(list(DRIVER_VEHICLE))
        vehicle_id = DRIVER_VEHICLE[driver_id]
    if status == "completed":
        # a driver either accepts the offer as-is or counter-offers higher
        counter = random.random() < 0.4
        final_price = round(offered_price * random.uniform(1.05, 1.30)) if counter else offered_price

    ride = {
        "ride_id": f"ride_{now.strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}",
        "requested_at": now.isoformat(),
        "pickup_zone_id": pickup,
        "dropoff_zone_id": dropoff,
        "status": status,
        "offered_price": offered_price,
        "final_price": final_price,
        "distance_km": distance_km if status == "completed" else None,
        "driver_id": driver_id,
        "vehicle_id": vehicle_id,
    }

    if random.random() < BAD_REFERENCE_RATE:
        if driver_id is not None and random.random() < 0.5:
            ride["driver_id"] = BAD_ID
        else:
            ride["pickup_zone_id"] = BAD_ID
    return ride


def write_atomically(ride):
    """Write to *.json.tmp then rename: the loader only globs *.json, so it
    can never pick up a half-written file."""
    path = os.path.join(OUT_DIR, f"{ride['ride_id']}.json")
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(ride, f)
    os.replace(tmp_path, path)
    return path


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    print(f"[simulate_rides] writing to {OUT_DIR}, every {PULL_INTERVAL_SECONDS}s", flush=True)
    while True:
        try:
            ride = build_ride_request()
            path = write_atomically(ride)
            print(
                f"[simulate_rides] {ride['ride_id']} {ride['status']:<9} "
                f"offered={ride['offered_price']} final={ride['final_price']} -> {path}",
                flush=True,
            )
        except Exception as exc:  # pragma: no cover - visible in `docker logs`
            print(f"[simulate_rides] ERROR: {exc}", file=sys.stderr, flush=True)
        time.sleep(PULL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
