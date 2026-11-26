"""Memuat data mentah OpenAQ ke DuckDB (schema `raw`). Idempotent: upsert by primary key.

`_loaded_at` di-set eksplisit: INSERT OR REPLACE tidak memakai DEFAULT saat baris sudah ada.
"""
import json
import os
from collections.abc import Iterable
from pathlib import Path

import duckdb

# Default: <root>/data/openaq.duckdb. Di container Airflow di-override via env OPENAQ_DB_PATH.
DB_PATH = Path(
    os.getenv("OPENAQ_DB_PATH", Path(__file__).resolve().parent.parent / "data" / "openaq.duckdb")
)

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.locations (
    location_id   BIGINT PRIMARY KEY,
    name          VARCHAR,
    country_code  VARCHAR,
    timezone      VARCHAR,
    latitude      DOUBLE,
    longitude     DOUBLE,
    is_mobile     BOOLEAN,
    is_monitor    BOOLEAN,
    provider_name VARCHAR,
    datetime_first_utc TIMESTAMPTZ,
    datetime_last_utc  TIMESTAMPTZ,
    payload       JSON,
    _loaded_at    TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.sensors (
    sensor_id      BIGINT PRIMARY KEY,
    location_id    BIGINT,
    name           VARCHAR,
    parameter_id   INTEGER,
    parameter_name VARCHAR,
    units          VARCHAR,
    _loaded_at     TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.measurements_hourly (
    sensor_id        BIGINT,
    datetime_from_utc TIMESTAMPTZ,
    datetime_to_utc   TIMESTAMPTZ,
    value            DOUBLE,
    parameter_name   VARCHAR,
    units            VARCHAR,
    min              DOUBLE,
    max              DOUBLE,
    avg              DOUBLE,
    sd               DOUBLE,
    percent_coverage DOUBLE,
    has_flags        BOOLEAN,
    payload          JSON,
    _loaded_at       TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (sensor_id, datetime_from_utc)
);
"""


def _g(d: dict | None, *keys):
    """Ambil nilai nested dengan aman: _g(x, 'a', 'b') -> x['a']['b'] atau None."""
    for k in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d


def connect(path: Path | str = DB_PATH) -> duckdb.DuckDBPyConnection:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    con.execute(DDL)
    return con


def load_locations(con: duckdb.DuckDBPyConnection, locations: Iterable[dict]) -> tuple[int, int]:
    """Upsert locations + sensors. Return (jumlah lokasi, jumlah sensor)."""
    loc_rows, sensor_rows = [], []
    for loc in locations:
        loc_rows.append((
            loc["id"], loc.get("name"), _g(loc, "country", "code"), loc.get("timezone"),
            _g(loc, "coordinates", "latitude"), _g(loc, "coordinates", "longitude"),
            loc.get("isMobile"), loc.get("isMonitor"), _g(loc, "provider", "name"),
            _g(loc, "datetimeFirst", "utc"), _g(loc, "datetimeLast", "utc"),
            json.dumps(loc),
        ))
        for s in loc.get("sensors") or []:
            sensor_rows.append((
                s["id"], loc["id"], s.get("name"), _g(s, "parameter", "id"),
                _g(s, "parameter", "name"), _g(s, "parameter", "units"),
            ))

    if not loc_rows:  # DuckDB executemany menolak list kosong
        return 0, 0
    con.executemany(
        """INSERT OR REPLACE INTO raw.locations
           (location_id, name, country_code, timezone, latitude, longitude, is_mobile,
            is_monitor, provider_name, datetime_first_utc, datetime_last_utc, payload, _loaded_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?, now())""",
        loc_rows,
    )
    if sensor_rows:
        con.executemany(
            """INSERT OR REPLACE INTO raw.sensors
               (sensor_id, location_id, name, parameter_id, parameter_name, units, _loaded_at)
               VALUES (?,?,?,?,?,?, now())""",
            sensor_rows,
        )
    return len(loc_rows), len(sensor_rows)


def load_hours(con: duckdb.DuckDBPyConnection, sensor_id: int, rows: Iterable[dict]) -> int:
    """Upsert agregat per jam untuk satu sensor. Return jumlah baris."""
    data = [
        (
            sensor_id, _g(r, "period", "datetimeFrom", "utc"), _g(r, "period", "datetimeTo", "utc"),
            r.get("value"), _g(r, "parameter", "name"), _g(r, "parameter", "units"),
            _g(r, "summary", "min"), _g(r, "summary", "max"), _g(r, "summary", "avg"),
            _g(r, "summary", "sd"), _g(r, "coverage", "percentCoverage"),
            _g(r, "flagInfo", "hasFlags"), json.dumps(r),
        )
        for r in rows
    ]
    if not data:  # sensor tanpa data di window ini; DuckDB executemany menolak list kosong
        return 0
    con.executemany(
        """INSERT OR REPLACE INTO raw.measurements_hourly
           (sensor_id, datetime_from_utc, datetime_to_utc, value, parameter_name, units,
            min, max, avg, sd, percent_coverage, has_flags, payload, _loaded_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?, now())""",
        data,
    )
    return len(data)
