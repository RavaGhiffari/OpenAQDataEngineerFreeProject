import pytest

from Handler.loader import connect


@pytest.fixture
def con():
    """DuckDB in-memory dengan schema raw.* — tidak menyentuh data/openaq.duckdb."""
    c = connect(":memory:")
    yield c
    c.close()


def make_location(loc_id=1, name="Jakarta", sensors=None, **overrides):
    """Bentuk JSON mirip respons /v3/locations (hanya field yang dipakai loader)."""
    loc = {
        "id": loc_id,
        "name": name,
        "country": {"code": "ID"},
        "timezone": "Asia/Jakarta",
        "coordinates": {"latitude": -6.2, "longitude": 106.8},
        "isMobile": False,
        "isMonitor": True,
        "provider": {"name": "AirNow"},
        "datetimeFirst": {"utc": "2016-01-01T00:00:00Z"},
        "datetimeLast": {"utc": "2026-11-28T00:00:00Z"},
        "sensors": sensors if sensors is not None else [
            {"id": 10, "name": "pm25 µg/m³",
             "parameter": {"id": 2, "name": "pm25", "units": "µg/m³"}},
        ],
    }
    loc.update(overrides)
    return loc


def make_hour(hour="2026-11-28T00:00:00Z", value=20.0):
    """Bentuk JSON mirip respons /v3/sensors/{id}/hours."""
    return {
        "value": value,
        "parameter": {"name": "pm25", "units": "µg/m³"},
        "period": {"datetimeFrom": {"utc": hour}, "datetimeTo": {"utc": hour}},
        "summary": {"min": value - 1, "max": value + 1, "avg": value, "sd": 0.5},
        "coverage": {"percentCoverage": 100.0},
        "flagInfo": {"hasFlags": False},
    }
