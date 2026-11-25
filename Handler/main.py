"""Orkestrator: ambil data dari OpenAQ lewat client, simpan ke DuckDB lewat loader.

Jalankan dari root proyek:
    python -m Handler.main
    python -m Handler.main --country-id 1 --sensor-id 5047 --from 2016-01-30 --to 2016-02-06
"""
import argparse

from Handler.client import OpenAQClient
from Handler.loader import connect, load_hours, load_locations


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load data OpenAQ ke DuckDB (schema raw)")
    p.add_argument("--country-id", type=int, default=1, help="ID negara OpenAQ (lihat /countries)")
    p.add_argument("--sensor-id", type=int, default=5047, help="Sensor untuk data per jam")
    p.add_argument("--from", dest="date_from", default="2016-01-30", help="Tanggal awal ISO")
    p.add_argument("--to", dest="date_to", default="2016-02-05", help="Tanggal akhir ISO")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    con = connect()  # membuat openaq.duckdb + tabel raw.* bila belum ada

    with OpenAQClient() as client:
        n_loc, n_sensor = load_locations(con, client.locations(countries_id=args.country_id))
        print(f"locations: {n_loc}, sensors: {n_sensor}")

        n_hours = load_hours(
            con, args.sensor_id, client.hours(args.sensor_id, args.date_from, args.date_to)
        )
        print(f"hourly rows (sensor {args.sensor_id}): {n_hours}")

    print(con.sql("SELECT count(*) AS locations FROM raw.locations"))
    print(con.sql("SELECT count(*) AS sensors FROM raw.sensors"))
    con.close()


if __name__ == "__main__":
    main()
