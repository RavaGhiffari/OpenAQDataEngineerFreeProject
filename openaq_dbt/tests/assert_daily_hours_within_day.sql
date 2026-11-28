-- Satu hari maksimal 24 jam unik. Lebih dari itu berarti n_hours salah hitung (mis. sensor-jam).
select location_id, date_local, n_hours
from {{ ref('fct_pm25_daily') }}
where n_hours < 1 or n_hours > 24
