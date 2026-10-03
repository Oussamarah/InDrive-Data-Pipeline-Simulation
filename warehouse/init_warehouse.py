"""One-shot build step, run by the warehouse-init service before Airflow
starts: creates warehouse/indrive.duckdb from warehouse/schema.sql, then
exits. The DAG never runs DDL -- it only INSERTs/UPDATEs into tables this
script already created."""
import duckdb

WAREHOUSE_PATH = "/opt/warehouse/indrive.duckdb"
SCHEMA_PATH = "/opt/warehouse/schema.sql"


def main():
    con = duckdb.connect(WAREHOUSE_PATH)
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        con.execute(f.read())
    tables = sorted(r[0] for r in con.execute("SHOW TABLES").fetchall())
    con.close()
    print(f"[init_warehouse] {WAREHOUSE_PATH} ready -- {len(tables)} table(s): {tables}")


if __name__ == "__main__":
    main()
