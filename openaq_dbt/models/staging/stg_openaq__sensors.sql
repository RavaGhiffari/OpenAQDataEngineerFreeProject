select
    sensor_id,
    location_id,
    name                    as sensor_name,
    parameter_id,
    lower(parameter_name)   as parameter_name,
    units,
    _loaded_at
from {{ source('openaq', 'sensors') }}
