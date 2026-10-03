"""inDrive rides pipeline -- ONE DAG, three stages, thin by design: all the
logic (upsert, archive, quarantine, audit, SCD strategies) lives in
scripts/load.py and scripts/transform/*.sql. This file only wires tasks in
the right order:

  [load_batch, load_streaming]                    -- land raw files (parallel)
        >> [dim_zone, dim_vehicle, dim_driver]    -- upsert dimensions (parallel,
                                                     each a different SCD type)
        >> fact_ride_requests                     -- needs all three dimensions
                                                     current before it can join

dim_date and dim_time are NOT tasks here -- they're static calendars, seeded
once by the warehouse-init service."""
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/scripts")

# a locked warehouse file is transient -- retry before giving up, instead of
# waiting for the next scheduled run
DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}


def _load_batch():
    from load import load_batch

    load_batch()


def _load_streaming():
    from load import load_streaming

    load_streaming()


def _run_sql(filename):
    def _callable():
        from run_sql import run_sql_file

        run_sql_file(filename)

    return _callable


with DAG(
    dag_id="indrive_pipeline",
    description="Load batch + streaming sources, then upsert the Kimball star schema",
    schedule_interval="*/2 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # DuckDB has one writer at a time: two overlapping runs would just fight
    # over the lock, so a slow run delays the next one instead
    max_active_runs=1,
    tags=["indrive"],
    default_args=DEFAULT_ARGS,
) as dag:

    t_load_batch = PythonOperator(task_id="load_batch", python_callable=_load_batch)
    t_load_streaming = PythonOperator(task_id="load_streaming", python_callable=_load_streaming)

    t_dim_zone = PythonOperator(task_id="dim_zone", python_callable=_run_sql("dim_zone.sql"))
    t_dim_vehicle = PythonOperator(task_id="dim_vehicle", python_callable=_run_sql("dim_vehicle.sql"))
    t_dim_driver = PythonOperator(task_id="dim_driver", python_callable=_run_sql("dim_driver.sql"))

    t_fact_ride_requests = PythonOperator(
        task_id="fact_ride_requests", python_callable=_run_sql("fact_ride_requests.sql")
    )

    # Airflow's >> doesn't support list >> list directly, so each load task
    # fans out to all three dimensions individually -- same dependency graph
    # as [load_batch, load_streaming] >> [dim_zone, dim_vehicle, dim_driver]
    dims = [t_dim_zone, t_dim_vehicle, t_dim_driver]
    t_load_batch >> dims
    t_load_streaming >> dims
    dims >> t_fact_ride_requests
