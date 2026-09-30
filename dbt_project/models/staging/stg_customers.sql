-- Cleaned customer snapshots: one row per customer per monthly batch.
-- Fixes the source system's messy input (spaces, casing, empty strings).

with source as (
    select * from {{ source('raw', 'customers') }}
)

select
    customer_id,
    trim(first_name)                       as first_name,
    trim(last_name)                        as last_name,
    date_of_birth::date                    as date_of_birth,
    nullif(lower(trim(email)), '')         as email,
    trim(phone)                            as phone,
    trim(street_address)                   as street_address,
    trim(city)                             as city,
    upper(trim(state))                     as state,
    trim(postal_code)                      as postal_code,
    upper(trim(risk_rating))               as risk_rating,
    customer_since::date                   as customer_since,
    last_updated::timestamp                as last_updated_at,

    -- audit / lineage columns carried through from raw
    _batch_id,
    _source_file,
    _source_row,
    _loaded_at
from source
