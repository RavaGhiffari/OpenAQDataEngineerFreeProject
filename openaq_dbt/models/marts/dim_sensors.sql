select
    s.sensor_id,
    s.location_id,
    l.location_name,
    s.sensor_name,
    s.parameter_id,
    s.parameter_name,
    s.units
from {{ ref('stg_openaq__sensors') }} s
left join {{ ref('stg_openaq__locations') }} l using (location_id)
