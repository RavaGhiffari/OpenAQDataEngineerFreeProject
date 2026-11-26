select
    location_id,
    trim(name)                              as location_name,
    upper(country_code)                     as country_code,
    timezone,
    latitude,
    longitude,
    coalesce(is_mobile, false)              as is_mobile,
    coalesce(is_monitor, false)             as is_monitor,
    provider_name,
    datetime_first_utc at time zone 'UTC'   as first_seen_at_utc,
    datetime_last_utc  at time zone 'UTC'   as last_seen_at_utc,
    _loaded_at
from {{ source('openaq', 'locations') }}
