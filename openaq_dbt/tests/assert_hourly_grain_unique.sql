-- Satu baris per sensor per jam di fact.
select sensor_id, measured_at_utc, count(*) as n
from {{ ref('fct_pm25_hourly') }}
group by all
having count(*) > 1
