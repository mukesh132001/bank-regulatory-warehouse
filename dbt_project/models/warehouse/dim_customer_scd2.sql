-- SCD Type 2: full history of TRACKED attributes (address + risk rating).
-- A new version starts only when a tracked attribute changes.
-- Email/phone are Type 1 (see dim_customer) and do NOT create new versions.

with snapshots as (
    select
        customer_id,
        street_address,
        city,
        state,
        postal_code,
        risk_rating,
        customer_since,
        last_updated_at,
        _batch_id,
        _source_file,
        -- one fingerprint of all tracked columns, so we compare them in one go
        md5(concat_ws('|', street_address, city, state, postal_code, risk_rating)) as tracked_hash
    from {{ ref('stg_customers') }}
),

compared as (
    select
        *,
        lag(tracked_hash) over (partition by customer_id order by _batch_id) as previous_hash
    from snapshots
),

versions as (
    -- the first row per customer, plus every row where tracked values changed
    select
        *,
        case when previous_hash is null then customer_since::timestamp
             else last_updated_at end as valid_from
    from compared
    where previous_hash is null
       or tracked_hash <> previous_hash
)

select
    md5(customer_id || '|' || _batch_id)                       as customer_sk,
    customer_id,
    street_address,
    city,
    state,
    postal_code,
    risk_rating,
    valid_from,
    coalesce(
        lead(valid_from) over (partition by customer_id order by valid_from),
        '9999-12-31'::timestamp
    )                                                          as valid_to,
    lead(valid_from) over (partition by customer_id order by valid_from) is null
                                                               as is_current,
    _batch_id                                                  as first_seen_batch,
    _source_file
from versions
