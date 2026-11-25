from Handler.client import OpenAQClient
from Handler.loader import connect, load_locations, load_hours

SENSOR_ID = 5047  # PM2.5, US Diplomatic Post Jakarta South

con = connect()  # menulis ke openaq.duckdb
with OpenAQClient() as c:
    print("locations, sensors:", load_locations(con, c.locations(countries_id=1)))
    print("hourly rows:", load_hours(con, SENSOR_ID, c.hours(SENSOR_ID, "2016-01-30", "2016-02-05")))

print(con.sql("SELECT count(*) AS locations FROM raw.locations"))
print(con.sql("SELECT count(*) AS sensors FROM raw.sensors"))
print(con.sql("""
    SELECT sensor_id, datetime_from_utc, value, percent_coverage
    FROM raw.measurements_hourly ORDER BY datetime_from_utc LIMIT 5
"""))
