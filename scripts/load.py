"""Load step -- thin by design: raw_* tables and load_audit already exist
(created once by the warehouse-init service from warehouse/schema.sql), so
this module only ever INSERTs data and moves files. No CREATE/DROP/REPLACE
TABLE here -- the star schema is owned exclusively by the transform tasks.

Every file is moved out of data-lake/input/... after being processed,
success or failure, so the next run never sees it again:
  - success    -> data-lake/archive/<batch|streaming>/<name>_<timestamp><ext>
  - bad file   -> data-lake/quarantine/<batch|streaming>/<name>_<timestamp><ext>
and gets exactly one row in load_audit either way -- that row is the
tracking/auditing trail: which file, when, how many rows, and why it
failed if it did.
"""
import glob
import os
import shutil
from datetime import datetime

import duckdb

LAKE_ROOT = os.environ.get("LAKE_ROOT", "/opt/airflow/data-lake")
WAREHOUSE_PATH = os.environ.get("WAREHOUSE_PATH", "/opt/airflow/warehouse/indrive.duckdb")

# file name -> (table, expected columns). Natural key per table below.
BATCH_FILES = {
    "zones.csv": ("raw_zones", ["zone_id", "zone_name", "city"]),
    "drivers.csv": (
        "raw_drivers",
        ["driver_id", "driver_name", "phone", "city", "rating", "joined_date"],
    ),
    "vehicles.csv": (
        "raw_vehicles",
        ["vehicle_id", "driver_id", "make", "model", "plate_number", "vehicle_type"],
    ),
}
BATCH_PK = {"raw_zones": "zone_id", "raw_drivers": "driver_id", "raw_vehicles": "vehicle_id"}

STREAMING_COLUMNS = [
    "ride_id", "requested_at", "pickup_zone_id", "dropoff_zone_id", "status",
    "offered_price", "final_price", "distance_km", "driver_id", "vehicle_id",
]
# driver/vehicle/price/distance are legitimately null for some statuses,
# but the fields themselves must be present in every event
STREAMING_REQUIRED_FIELDS = set(STREAMING_COLUMNS)


def _timestamped_name(path):
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S%f")
    name, ext = os.path.splitext(os.path.basename(path))
    return f"{name}_{ts}{ext}"


def _move(path, dest_dir):
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, _timestamped_name(path))
    shutil.move(path, dest)
    return dest


def _audit(con, source_type, file_name, status, row_count, error_message=None):
    con.execute(
        "INSERT INTO load_audit (source_type, file_name, status, row_count, error_message) "
        "VALUES (?, ?, ?, ?, ?)",
        [source_type, file_name, status, row_count, error_message],
    )


def load_batch():
    """Upsert each referentiel CSV by its natural key -- a re-run with an
    updated file just updates existing rows and inserts new ones."""
    input_dir = os.path.join(LAKE_ROOT, "input", "batch")
    archive_dir = os.path.join(LAKE_ROOT, "archive", "batch")
    quarantine_dir = os.path.join(LAKE_ROOT, "quarantine", "batch")

    con = duckdb.connect(WAREHOUSE_PATH)
    found = 0
    for fname, (table, cols) in BATCH_FILES.items():
        path = os.path.join(input_dir, fname)
        if not os.path.exists(path):
            continue
        found += 1
        try:
            # all_varchar: let the INSERT cast into the table's own types
            # instead of trusting type sniffing -- otherwise phone
            # '0661020304' is sniffed as a number and loses its leading 0.
            # A value that can't be cast (e.g. rating 'abc') fails the
            # INSERT and the file is quarantined.
            df = con.execute(
                "SELECT * FROM read_csv_auto(?, header=True, all_varchar=True)", [path]
            ).df()
            missing = set(cols) - set(df.columns)
            if missing:
                raise ValueError(f"missing column(s): {sorted(missing)}")

            row_count = len(df)
            pk = BATCH_PK[table]
            col_list = ", ".join(cols)
            set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != pk)

            con.register("_incoming", df)
            con.execute(
                f"""
                INSERT INTO {table} ({col_list}, source_file, loaded_at)
                SELECT {col_list}, ?, now() FROM _incoming
                ON CONFLICT ({pk}) DO UPDATE SET {set_clause},
                    source_file = EXCLUDED.source_file, loaded_at = EXCLUDED.loaded_at
                """,
                [fname],
            )
            con.unregister("_incoming")

            _audit(con, "batch", fname, "success", row_count)
            _move(path, archive_dir)
            print(f"[load_batch] {fname}: {row_count} row(s) upserted into {table}")
        except Exception as exc:
            _audit(con, "batch", fname, "failed", 0, str(exc))
            _move(path, quarantine_dir)
            print(f"[load_batch] {fname}: QUARANTINED -- {exc}")
    con.close()
    print(f"[load_batch] {found} file(s) found in {input_dir}")


def load_streaming():
    """Append-only: each ride request is an immutable event. ride_id is the
    PRIMARY KEY of raw_ride_requests, so re-loading an already-seen file is
    a no-op instead of a duplicate row."""
    input_dir = os.path.join(LAKE_ROOT, "input", "streaming")
    archive_dir = os.path.join(LAKE_ROOT, "archive", "streaming")
    quarantine_dir = os.path.join(LAKE_ROOT, "quarantine", "streaming")

    # *.json only: the simulator writes *.json.tmp first and renames when
    # done, so a half-written file is never picked up
    new_files = sorted(glob.glob(os.path.join(input_dir, "*.json")))
    print(f"[load_streaming] {len(new_files)} new ride request file(s) in {input_dir}")
    if not new_files:
        return

    con = duckdb.connect(WAREHOUSE_PATH)
    col_list = ", ".join(STREAMING_COLUMNS)
    for path in new_files:
        fname = os.path.basename(path)
        try:
            df = con.execute("SELECT * FROM read_json_auto(?)", [path]).df()
            missing = STREAMING_REQUIRED_FIELDS - set(df.columns)
            if missing:
                raise ValueError(f"missing field(s): {sorted(missing)}")

            row_count = len(df)
            con.execute(
                f"""
                INSERT INTO raw_ride_requests ({col_list}, source_file, loaded_at)
                SELECT {col_list}, ?, now()
                FROM read_json_auto(?)
                ON CONFLICT (ride_id) DO NOTHING
                """,
                [fname, path],
            )

            _audit(con, "streaming", fname, "success", row_count)
            _move(path, archive_dir)
        except Exception as exc:
            _audit(con, "streaming", fname, "failed", 0, str(exc))
            _move(path, quarantine_dir)
            print(f"[load_streaming] {fname}: QUARANTINED -- {exc}")
    con.close()
    print(f"[load_streaming] {len(new_files)} file(s) processed")
