"""Test OpenAQClient tanpa jaringan: transport httpx diganti MockTransport."""
import httpx
import pytest

import Handler.client as client_mod
from Handler.client import BASE_URL, OpenAQClient


@pytest.fixture(autouse=True)
def no_sleep_no_dotenv(monkeypatch):
    sleeps = []
    monkeypatch.setattr(client_mod.time, "sleep", sleeps.append)
    monkeypatch.setattr(client_mod, "load_dotenv", lambda *a, **k: None)  # abaikan .env lokal
    return sleeps


def make_client(handler, **kwargs) -> OpenAQClient:
    c = OpenAQClient(api_key="test-key", **kwargs)
    c._http = httpx.Client(
        base_url=BASE_URL,
        headers={"X-API-Key": "test-key"},
        transport=httpx.MockTransport(handler),
    )
    return c


def test_missing_api_key_raises(monkeypatch):
    monkeypatch.delenv("OPENAQ_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="OPENAQ_API_KEY"):
        OpenAQClient()


def test_get_sends_api_key_and_drops_none_params():
    seen = {}

    def handler(req):
        seen["key"] = req.headers["X-API-Key"]
        seen["params"] = dict(req.url.params)
        return httpx.Response(200, json={"results": []})

    with make_client(handler) as c:
        c.get("/locations", countries_id=1, datetime_from=None)

    assert seen["key"] == "test-key"
    assert seen["params"] == {"countries_id": "1"}


def test_paginate_follows_pages_until_short_page():
    pages = {1: [{"id": 1}, {"id": 2}], 2: [{"id": 3}, {"id": 4}], 3: [{"id": 5}]}
    requested = []

    def handler(req):
        page = int(req.url.params["page"])
        requested.append(page)
        return httpx.Response(200, json={"results": pages[page]})

    with make_client(handler) as c:
        items = list(c.paginate("/locations", limit=2))

    assert [i["id"] for i in items] == [1, 2, 3, 4, 5]
    assert requested == [1, 2, 3]


def test_retries_on_5xx_with_exponential_backoff(no_sleep_no_dotenv):
    responses = iter([httpx.Response(503), httpx.Response(500), httpx.Response(200, json={"ok": 1})])

    with make_client(lambda req: next(responses)) as c:
        assert c.get("/x") == {"ok": 1}

    assert no_sleep_no_dotenv == [1, 2]  # 2**0, 2**1


def test_429_waits_for_ratelimit_reset(no_sleep_no_dotenv):
    responses = iter([
        httpx.Response(429, headers={"x-ratelimit-reset": "7"}),
        httpx.Response(200, json={"ok": 1}),
    ])

    with make_client(lambda req: next(responses)) as c:
        c.get("/x")

    assert no_sleep_no_dotenv == [8.0]  # reset + 1 detik


def test_waits_when_quota_exhausted(no_sleep_no_dotenv):
    def handler(req):
        return httpx.Response(
            200, json={}, headers={"x-ratelimit-remaining": "0", "x-ratelimit-reset": "3"}
        )

    with make_client(handler) as c:
        c.get("/x")

    assert no_sleep_no_dotenv == [4.0]


def test_gives_up_after_max_retries():
    with make_client(lambda req: httpx.Response(503), max_retries=2) as c:
        with pytest.raises(httpx.HTTPStatusError):
            c.get("/x")


def test_non_retryable_error_raises_immediately(no_sleep_no_dotenv):
    calls = []

    def handler(req):
        calls.append(1)
        return httpx.Response(401)

    with make_client(handler) as c:
        with pytest.raises(httpx.HTTPStatusError):
            c.get("/x")

    assert len(calls) == 1
    assert no_sleep_no_dotenv == []


def test_retries_transport_error():
    calls = []

    def handler(req):
        calls.append(1)
        if len(calls) == 1:
            raise httpx.ConnectError("boom")
        return httpx.Response(200, json={"ok": 1})

    with make_client(handler) as c:
        assert c.get("/x") == {"ok": 1}
    assert len(calls) == 2


def test_hours_hits_sensor_endpoint():
    seen = {}

    def handler(req):
        seen["path"] = req.url.path
        seen["params"] = dict(req.url.params)
        return httpx.Response(200, json={"results": []})

    with make_client(handler) as c:
        list(c.hours(5047, "2026-11-27", "2026-11-28"))

    assert seen["path"] == "/v3/sensors/5047/hours"
    assert seen["params"]["datetime_from"] == "2026-11-27"
    assert seen["params"]["datetime_to"] == "2026-11-28"
