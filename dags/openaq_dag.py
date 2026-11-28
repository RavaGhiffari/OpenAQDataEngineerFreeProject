"""DAG harian: ingest OpenAQ (locations, sensors, hourly PM2.5) ke DuckDB schema `raw`,
lalu cek freshness source dan transformasi dbt (staging -> marts).

Catatan DuckDB: hanya satu proses boleh menulis ke file .duckdb. Karena itu task berjalan
berurutan dan max_active_runs=1. Setiap task membuka lalu menutup koneksinya sendiri.
"""
import json
from datetime import datetime, timedelta

import pendulum
from airflow.sdk import dag, task

COUNTRY_ID = 1          # Indonesia
PARAMETER = "pm25"
ACTIVE_WITHIN_DAYS = 7  # hanya sensor yang lokasinya masih kirim data belakangan ini

DBT = "/opt/dbt_venv/bin/dbt"
DBT_PATHS = "--target-path /tmp/dbt_target --log-path /tmp/dbt_logs"


@dag(
    dag_id="openaq_ingest",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["openaq", "duckdb"],
)
def openaq_ingest():

    @task
    def load_locations_task() -> dict:
        from Handler.client import OpenAQClient
        from Handler.loader import connect, load_locations

        con = connect()
        try:
            with OpenAQClient() as client:
                n_loc, n_sensor = load_locations(con, client.locations(countries_id=COUNTRY_ID))
        finally:
            con.close()  # lepas lock file DuckDB walaupun task gagal
        print(f"locations={n_loc} sensors={n_sensor}")
        return {"locations": n_loc, "sensors": n_sensor}

    @task
    def load_hours_task(**context) -> int:
        from Handler.client import OpenAQClient
        from Handler.loader import connect, load_hours

        # Run terjadwal: pakai data interval Airflow. Run manual (interval kosong/0): 1 hari terakhir.
        end = context.get("data_interval_end") or pendulum.now("UTC")
        start = context.get("data_interval_start")
        if start is None or start >= end:
            start = end.subtract(days=1)
        date_from, date_to = start.isoformat(), end.isoformat()
        print(f"window: {date_from} -> {date_to}")

        con = connect()
        try:
            sensor_ids = [r[0] for r in con.execute(
                """
                SELECT s.sensor_id
                FROM raw.sensors s JOIN raw.locations l USING (location_id)
                WHERE s.parameter_name = ?
                  AND l.datetime_last_utc >= CAST(? AS TIMESTAMPTZ) - to_days(CAST(? AS INTEGER))
                ORDER BY s.sensor_id
                """,
                # Relatif ke akhir window (bukan now()) agar rerun/backfill tanggal lama
                # tetap memilih sensor yang aktif pada periode itu.
                [PARAMETER, date_to, ACTIVE_WITHIN_DAYS],
            ).fetchall()]
            print(f"{len(sensor_ids)} sensor {PARAMETER} aktif")

            total = 0
            with OpenAQClient() as client:
                for sid in sensor_ids:
                    n = load_hours(con, sid, client.hours(sid, date_from, date_to))
                    print(f"sensor {sid}: {n} rows")
                    total += n
        finally:
            con.close()
        return total

    # Task dbt: retries=0 karena gagal = masalah data, bukan error sementara; retry tidak membantu.
    # target/ dan logs/ ditaruh di /tmp agar folder project yang di-mount dari host tetap bersih.

    @task.bash(retries=0)
    def dbt_source_freshness_task() -> str:
        # Gagal (error_after di _sources.yml) bila API berhenti mengirim jam baru,
        # walaupun task load sendiri sukses. Warn tidak menggagalkan task.
        return f'cd "$DBT_PROJECT_DIR" && {DBT} source freshness {DBT_PATHS}'

    @task.bash(retries=0)
    def dbt_build_task(**context) -> str:
        # `dbt build` = run + test sesuai urutan DAG; test gagal -> task gagal.
        # as_of_utc = akhir data interval -> is_active dkk deterministik per run.
        as_of = (context.get("data_interval_end") or pendulum.now("UTC")).isoformat()
        vars_json = json.dumps({"as_of_utc": as_of})
        return f"cd \"$DBT_PROJECT_DIR\" && {DBT} build {DBT_PATHS} --vars '{vars_json}'"

    (
        load_locations_task()
        >> load_hours_task()
        >> dbt_source_freshness_task()
        >> dbt_build_task()
    )


openaq_ingest()
