{{ config(severity='warn') }}

-- The ZIP code must belong to the range of the state of the address.
-- The reference ranges come from the zip_ranges seed. The generator associates random ZIP codes with the states:
-- the issue is monitored as a warning, the source data cannot be fixed in the silver layer.
select
    u.user_id,
    u.state,
    u.zip_code,
    z.zip_min,
    z.zip_max
from {{ ref('stg_users') }} u
left join {{ ref('zip_ranges') }} z on u.state = z.state
where z.state is null
   or u.zip_code is null
   or u.zip_code < z.zip_min
   or u.zip_code > z.zip_max
