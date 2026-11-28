import json

from Handler.loader import _g, load_hours, load_locations
from tests.conftest import make_hour, make_location


def count(con, table):
    return con.execute(f"SELECT count(*) FROM raw.{table}").fetchone()[0]


# --- _g -------------------------------------------------------------------

def test_g_reads_nested_value():
    assert _g({"a": {"b": 1}}, "a", "b") == 1


def test_g_returns_none_for_missing_or_non_dict():
    assert _g({"a": {}}, "a", "b") is None
    assert _g({"a": None}, "a", "b") is None
    assert _g({"a": 5}, "a", "b") is None
    assert _g(None, "a") is None


# --- load_locations ---------------------------------------------------------

def test_load_locations_inserts_locations_and_sensors(con):
    sensors = [
        {"id": 10, "parameter": {"id": 2, "name": "pm25", "units": "µg/m³"}},
        {"id": 11, "parameter": {"id": 1, "name": "pm10", "units": "µg/m³"}},
    ]
    assert load_locations(con, [make_location(1, sensors=sensors), make_location(2, sensors=[])]) == (2, 2)

    row = con.execute(
        "SELECT name, country_code, latitude, provider_name, payload FROM raw.locations WHERE location_id = 1"
    ).fetchone()
    assert row[:4] == ("Jakarta", "ID", -6.2, "AirNow")
    assert json.loads(row[4])["id"] == 1  # JSON asli disimpan utuh
    assert con.execute("SELECT location_id FROM raw.sensors WHERE sensor_id = 11").fetchone() == (1,)


def test_load_locations_is_idempotent_and_updates(con):
    load_locations(con, [make_location(1, name="Lama")])
    load_locations(con, [make_location(1, name="Baru")])

    assert count(con, "locations") == 1
    assert count(con, "sensors") == 1
    assert con.execute("SELECT name FROM raw.locations").fetchone() == ("Baru",)


def test_load_locations_refreshes_loaded_at_on_upsert(con):
    # Regresi: INSERT OR REPLACE tidak memakai DEFAULT now() saat baris sudah ada.
    load_locations(con, [make_location(1)])
    con.execute("UPDATE raw.locations SET _loaded_at = TIMESTAMPTZ '2000-01-01 00:00:00+00'")
    load_locations(con, [make_location(1)])

    (year,) = con.execute("SELECT year(_loaded_at) FROM raw.locations").fetchone()
    assert year > 2000


def test_load_locations_handles_missing_optional_fields(con):
    loc = {"id": 3}  # semua field lain hilang
    assert load_locations(con, [loc]) == (1, 0)
    assert con.execute("SELECT name, latitude FROM raw.locations").fetchone() == (None, None)


def test_load_locations_empty_input(con):
    assert load_locations(con, []) == (0, 0)
    assert load_locations(con, iter([])) == (0, 0)  # client mengembalikan generator


# --- load_hours -------------------------------------------------------------

def test_load_hours_inserts_rows(con):
    rows = [make_hour("2026-11-28T00:00:00Z", 20.0), make_hour("2026-11-28T01:00:00Z", 30.0)]
    assert load_hours(con, 10, rows) == 2

    got = con.execute(
        "SELECT sensor_id, value, parameter_name, percent_coverage, has_flags "
        "FROM raw.measurements_hourly ORDER BY datetime_from_utc"
    ).fetchall()
    assert got == [(10, 20.0, "pm25", 100.0, False), (10, 30.0, "pm25", 100.0, False)]


def test_load_hours_upserts_on_sensor_and_hour(con):
    load_hours(con, 10, [make_hour("2026-11-28T00:00:00Z", 20.0)])
    load_hours(con, 10, [make_hour("2026-11-28T00:00:00Z", 25.0)])  # jam sama, nilai direvisi
    load_hours(con, 11, [make_hour("2026-11-28T00:00:00Z", 99.0)])  # sensor lain, jam sama

    assert count(con, "measurements_hourly") == 2
    assert con.execute(
        "SELECT value FROM raw.measurements_hourly WHERE sensor_id = 10"
    ).fetchone() == (25.0,)


def test_load_hours_empty_input(con):
    assert load_hours(con, 10, iter([])) == 0
    assert count(con, "measurements_hourly") == 0
