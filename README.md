# inDrive Rides Pipeline: Airflow + DuckDB, Kimball star schema

Goal: land two sources (a **streaming** feed of simulated ride requests and **batch** CSV referentials), model them into a Kimball star schema in DuckDB, and orchestrate everything with one Airflow DAG. Everything runs **locally in Docker**.

The streaming source is random, so your row counts will differ from anyone else's. Judge results by shape and rules, not exact numbers.

```
data-lake/input/batch/*.csv ──┐                                        ┌─ dim_zone    (SCD 1)
                              ├─ indrive_pipeline (every 2 min): raw_* ┼─ dim_vehicle (SCD 3) ─► fact_ride_requests
data-lake/input/streaming/*.json ┘                                     └─ dim_driver  (SCD 2)    fact_ride_requests_rejected
        ▲                                                                 dim_date, dim_time (static, seeded at build)
   stream-consumer (1 ride request/sec)
```

Three concerns, three places:
- **Schema** (build time): `warehouse/schema.sql`, run once by the `warehouse-init` service. The DAG never runs DDL.
- **Logic**: `scripts/load.py` and `scripts/transform/*.sql`.
- **Orchestration**: `dags/indrive_pipeline.py`. It only decides when and in what order.

Data vs referentials: **data (facts)** = what happened, timestamped, never edited (each ride request). **Referentials (dimensions)** = what exists (zones, drivers, vehicles), low volume, updated in place.

**Fact grain: one row per ride request**, not per completed ride, so "what % of requests never found a driver?" is answerable. Finance questions filter `status = 'completed'`.

---

## 0. Prerequisites

| Need | Check |
|---|---|
| Docker Desktop running (with Compose v2) | `docker --version` and `docker compose version` |
| Free ports **8081** (Airflow) and **8090** (warehouse viewer) | nothing else on those ports |

Folder map: `warehouse/` (schema + Dockerfile + DuckDB file), `data-lake/` (input, archive, quarantine × batch/streaming), `scripts/` (task logic, shared by the DAG), `dags/` (one DAG), `airflow/` (Dockerfile that bakes duckdb+pandas into the Airflow image), `consumer/` (streaming source), `viewer/` (read-only browser UI).

Every folder is a **bind mount**, not a Docker-managed volume: you can open `warehouse/indrive.duckdb` or browse `data-lake/` directly. Containers share the `indrive_net` network and have plain names (`indrive-airflow`, `indrive-stream-consumer`, ...).

---

## Part A — Look at both sources (before starting anything)

```bash
head -n 3 data-lake/input/batch/zones.csv
head -n 3 data-lake/input/batch/drivers.csv
head -n 3 data-lake/input/batch/vehicles.csv
```

All three are referentials (dimensions). The fact table only appears when ride requests start arriving.

> Do this **before** Part C: once `load_batch` runs, the batch CSVs are moved to `data-lake/archive/batch/`.

---

## Part B — Start the streaming source

```bash
docker compose up -d --build stream-consumer
docker compose logs -f stream-consumer               # Ctrl+C stops following
ls data-lake/input/streaming/ | wc -l                # grows about once per second
```

Each JSON file = one ride request:

```json
{"ride_id": "ride_20261003154716_df8e7c76", "requested_at": "2026-10-03T15:47:16.950016",
 "pickup_zone_id": 3, "dropoff_zone_id": 3, "status": "completed",
 "offered_price": 14, "final_price": 14, "distance_km": 1.34, "driver_id": 2, "vehicle_id": 2}
```

How the simulator (`consumer/simulate_rides.py`) behaves:
- **inDrive pricing**: the passenger proposes `offered_price`; a driver accepts it or counter-offers higher, giving `final_price`. A lowball offer is far more likely to end as `no_driver`.
- `status`: `completed` (driver, final price, distance), `cancelled` (driver assigned, no price), `no_driver` (no driver/vehicle).
- `requested_at` is **UTC**. The warehouse converts it to Casablanca time itself (see Part D).
- About **5%** of requests reference `pickup_zone_id: 999` or `driver_id: 999` on purpose: Part D rejects them.
- Files are written as `*.json.tmp` then renamed, so the loader never reads a half-written file.

---

## Part C — Build the schema, then run the pipeline

```bash
docker compose up -d --build
docker compose logs warehouse-init
```

Expected: `[init_warehouse] /opt/warehouse/indrive.duckdb ready -- 12 table(s): [...]`.

```bash
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt   # admin password
# open http://localhost:8081   user: admin
docker compose exec airflow airflow dags trigger indrive_pipeline            # don't wait for the schedule
```

`dags/indrive_pipeline.py`: `load_batch` and `load_streaming` run in parallel, then the three dimension upserts in parallel, then `fact_ride_requests`. Schedule `*/2 * * * *`, 2 retries, `max_active_runs=1` (DuckDB has one writer at a time).

What `scripts/load.py` does:
- **Batch**: upserts each CSV by natural key (`ON CONFLICT ... DO UPDATE`). CSVs are read as text and cast by the INSERT, so `phone` keeps its leading `0`.
- **Streaming**: append-only, `ride_id` PRIMARY KEY with `ON CONFLICT DO NOTHING`.
- Success → file moved to `data-lake/archive/...` with a timestamp. Failure → `data-lake/quarantine/...`.
- Every file gets exactly one `load_audit` row.

### Try it: quarantine a bad file

```bash
echo '{not valid json' > data-lake/input/streaming/ride_broken.json
docker compose exec airflow airflow dags trigger indrive_pipeline
ls data-lake/quarantine/streaming/          # ride_broken_<timestamp>.json
```

At **http://localhost:8090/query**:
```sql
SELECT source_type, file_name, status, row_count, error_message FROM load_audit WHERE status = 'failed'
```

---

## Part D — The Kimball star schema

### Three dimensions, three SCD strategies

| Dimension | Type | Behavior |
|---|---|---|
| `dim_zone` | **Type 1** | Overwrite in place, no history |
| `dim_vehicle` | **Type 3** | Keeps one prior value: `vehicle_type` + `previous_vehicle_type` |
| `dim_driver` | **Type 2** | New row per version: `driver_key`, `valid_from`, `valid_to`, `is_current` |

`dim_driver` details (`scripts/transform/dim_driver.sql`):
- A change to name, phone, city, rating or joined_date closes the current version and opens a new one.
- A driver's **first** version is valid from `joined_date`, not from the first DAG run, so rides streamed before the pipeline first ran still find their driver.
- SCD windows are in UTC, the same clock as `requested_at`.

### Rules in `scripts/transform/fact_ride_requests.sql`
- **Rule 0**: only `raw_ride_requests WHERE processed_at IS NULL`, then stamp `processed_at`. A watermark, so re-runs don't double the fact. The whole step is one transaction.
- **Rule 1**: a request with an unknown status, zone, driver, vehicle, or a `completed` ride without a driver/price goes to `fact_ride_requests_rejected` with a `reason`.
- **Rule 2**: `driver_key` is the `dim_driver` version valid at the ride's own `requested_at` (the SCD Type 2 payoff).
- `date_key` and `hour` come from `requested_at_local` (Casablanca time), so `dim_time.is_peak` means local rush hour.

### Try it: change a driver, a vehicle and a zone

`load.py` moved the CSVs to `data-lake/archive/batch/`. Copy them back:
```bash
ls data-lake/archive/batch/
cp data-lake/archive/batch/drivers_<timestamp>.csv  data-lake/input/batch/drivers.csv
cp data-lake/archive/batch/vehicles_<timestamp>.csv data-lake/input/batch/vehicles.csv
cp data-lake/archive/batch/zones_<timestamp>.csv    data-lake/input/batch/zones.csv
```

Edit them:
- `drivers.csv`: driver 1's `rating` `4.8` → `4.2`.
- `vehicles.csv`: vehicle 1's `vehicle_type` `economy` → `comfort`.
- `zones.csv`: zone 1's `zone_name` `Maarif` → `Maarif Centre`.

```bash
docker compose exec airflow airflow dags trigger indrive_pipeline
```

```sql
SELECT driver_key, rating, valid_from, valid_to, is_current FROM dim_driver WHERE driver_id = 1 ORDER BY driver_key
```
```sql
SELECT vehicle_id, vehicle_type, previous_vehicle_type FROM dim_vehicle WHERE vehicle_id = 1
```
```sql
SELECT * FROM dim_zone WHERE zone_id = 1
```

Expected: driver 1 has **two** rows (old one closed, new one current); vehicle 1 has `previous_vehicle_type = 'economy'`; zone 1 was simply renamed (still 8 zones). Wait for the next run, then:

```sql
SELECT d.rating, count(*) AS rides, min(f.requested_at) AS first, max(f.requested_at) AS last
FROM fact_ride_requests f JOIN dim_driver d USING (driver_key)
WHERE f.driver_id = 1 GROUP BY 1
```
Rides before the change point at rating 4.8, rides after it at 4.2.

---

## Part E — Business questions

Open **http://localhost:8090** (tables + row counts) or **http://localhost:8090/query**. The viewer opens the warehouse **read-only**, so it never blocks the pipeline.

% of requests that never found a driver, by pickup zone:
```sql
SELECT z.zone_name,
       count(*) AS requests,
       round(100.0 * count(*) FILTER (WHERE f.status = 'no_driver') / count(*), 1) AS pct_no_driver
FROM fact_ride_requests f JOIN dim_zone z ON z.zone_id = f.pickup_zone_id
GROUP BY 1 ORDER BY pct_no_driver DESC
```

Requests by hour of day (Casablanca time):
```sql
SELECT t.hour, t.day_part, t.is_peak, count(*) AS requests
FROM fact_ride_requests f JOIN dim_time t ON t.hour = f.hour
GROUP BY ALL ORDER BY t.hour
```

The haggle: how often drivers counter-offer, and by how much:
```sql
SELECT count(*) AS completed_rides,
       round(100.0 * count(*) FILTER (WHERE price_gap > 0) / count(*), 1) AS pct_countered,
       round(avg(price_gap) FILTER (WHERE price_gap > 0), 2) AS avg_counter_mad
FROM fact_ride_requests WHERE status = 'completed'
```

Revenue per driver, current profile:
```sql
SELECT d.driver_name, d.rating, count(*) AS rides, sum(f.final_price) AS revenue_mad,
       round(sum(f.final_price) / sum(f.distance_km), 2) AS mad_per_km
FROM fact_ride_requests f JOIN dim_driver d ON d.driver_id = f.driver_id AND d.is_current
WHERE f.status = 'completed'
GROUP BY 1, 2 ORDER BY revenue_mad DESC
```

Reject rate against the ~5% design target:
```sql
SELECT reason, count(*) FROM fact_ride_requests_rejected GROUP BY 1
```

Load audit health check:
```sql
SELECT source_type, status, count(*) AS files, sum(row_count) AS rows_loaded
FROM load_audit GROUP BY 1, 2 ORDER BY 1, 2
```

### Invariants to verify
- Re-triggering `indrive_pipeline` twice in a row does **not** grow `fact_ride_requests`.
- `count(raw_ride_requests)` = `count(fact_ride_requests)` + `count(fact_ride_requests_rejected)`.
- Bad files end up in `data-lake/quarantine/`, not as DAG failures.
- Re-dropping an updated `drivers.csv` adds versions, never duplicates `is_current` rows (exactly 8 current drivers).
- `phone` values in `raw_drivers` keep their leading `0`.

---

## Cleanup

```bash
docker compose stop stream-consumer          # stop generating ride requests
docker compose down                          # stop everything, warehouse file and data lake stay
# full reset: delete warehouse/indrive.duckdb, empty data-lake/archive + data-lake/quarantine,
# put the CSVs back in data-lake/input/batch/, then `docker compose up -d --build`
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| Airflow UI not reachable | `docker compose logs airflow`; give it a few seconds after `docker compose up` |
| Can't find the admin password | `docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt` |
| `warehouse-init` failed | `docker compose logs warehouse-init`; `docker compose up -d --build` again |
| Viewer at :8090 says "Warehouse is busy" | A DAG task is mid-write; just refresh |
| `IO Error: Could not set lock on file` (DuckDB) | Another process holds the DB open (a Python REPL?). Close it |
| Batch CSVs missing from `input/batch` | They were archived after the first load: copy back from `data-lake/archive/batch/` |
| Port 8081 or 8090 in use | Change the left side of the matching `"host:container"` line in `docker-compose.yml` |
