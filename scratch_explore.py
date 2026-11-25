import os, json, httpx
from dotenv import load_dotenv

load_dotenv()
headers = {"X-API-Key": os.getenv("OPENAQ_API_KEY")}

r = httpx.get(
    "https://api.openaq.org/v3/locations",
    headers=headers,
    params={"limit": 5, "countries_id": 1},  # cari id Indonesia lewat /countries
    timeout=30,
)
print(r.status_code, r.headers.get("x-ratelimit-remaining"))
print(json.dumps(r.json(), indent=2)[:3000])