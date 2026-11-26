-- PM2.5 per sensor per jam. Baris tanpa nilai dan lokasi di luar Indonesia dibuang.
select
    m.sensor_id,
    s.location_id,
    m.measured_at_utc,
    timezone(l.timezone, timezone('UTC', m.measured_at_utc))  as measured_at_local,
    m.value                                                     as pm25_ugm3,
    m.percent_coverage,
    m.has_flags
from {{ ref('stg_openaq__measurements_hourly') }} m
join {{ ref('stg_openaq__sensors') }} s using (sensor_id)
join {{ ref('dim_locations') }}       l using (location_id)
where m.parameter_name = 'pm25'
  and m.value is not null
  and l.is_within_indonesia
