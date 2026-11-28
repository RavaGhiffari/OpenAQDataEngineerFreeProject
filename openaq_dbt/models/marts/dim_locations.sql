select
    location_id,
    location_name,
    country_code,
    timezone,
    latitude,
    longitude,
    is_mobile,
    is_monitor,
    provider_name,
    first_seen_at_utc,
    last_seen_at_utc,
    {{ as_of_utc() }}                                                 as as_of_utc,
    last_seen_at_utc >= {{ as_of_utc() }} - interval 7 day            as is_active,
    -- OpenAQ kadang salah tag negara (mis. lokasi 6315887 di Bulgaria bertag ID).
    -- Bounding box kasar Indonesia untuk menyaring koordinat yang jelas di luar.
    coalesce(
        latitude between -11.5 and 6.5 and longitude between 94.5 and 141.5,
        false
    )                                                                 as is_within_indonesia
from {{ ref('stg_openaq__locations') }}
