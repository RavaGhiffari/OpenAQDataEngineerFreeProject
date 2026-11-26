-- Waktu disimpan sebagai TIMESTAMP UTC (tanpa zona) agar tidak tergantung TimeZone sesi.
select
    sensor_id,
    datetime_from_utc at time zone 'UTC'   as measured_at_utc,
    datetime_to_utc   at time zone 'UTC'   as period_end_at_utc,
    lower(parameter_name)                  as parameter_name,
    units,
    value,
    min                                    as value_min,
    max                                    as value_max,
    avg                                    as value_avg,
    sd                                     as value_sd,
    percent_coverage,
    coalesce(has_flags, false)             as has_flags,
    _loaded_at
from {{ source('openaq', 'measurements_hourly') }}
