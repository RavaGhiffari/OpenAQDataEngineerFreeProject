-- Rata-rata harian PM2.5 per lokasi (hari menurut zona waktu lokal) + kategori ISPU.
{% set bp = var('ispu_pm25_breakpoints') %}

with daily as (
    select
        location_id,
        cast(measured_at_local as date)  as date_local,
        avg(pm25_ugm3)                   as pm25_avg,
        max(pm25_ugm3)                   as pm25_max,
        min(pm25_ugm3)                   as pm25_min,
        count(*)                         as n_hours,
        count(distinct sensor_id)        as n_sensors
    from {{ ref('fct_pm25_hourly') }}
    group by all
)

select
    *,
    n_hours >= 18 as is_complete_day,   -- ~75% jam terisi
    case
        when pm25_avg <= {{ bp[0] }} then 'Baik'
        when pm25_avg <= {{ bp[1] }} then 'Sedang'
        when pm25_avg <= {{ bp[2] }} then 'Tidak Sehat'
        when pm25_avg <= {{ bp[3] }} then 'Sangat Tidak Sehat'
        else 'Berbahaya'
    end as ispu_category
from daily
