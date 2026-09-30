-- SCD Type 1: one row per customer with the LATEST values.
-- When something changes, the new value simply overwrites the old one. No history.

with ranked as (
    select
        *,
        row_number() over (partition by customer_id order by _batch_id desc) as recency
    from {{ ref('stg_customers') }}
)

select
    customer_id,
    first_name,
    last_name,
    date_of_birth,
    email,
    phone,
    street_address,
    city,
    state,
    postal_code,
    risk_rating,
    customer_since,
    last_updated_at,
    _batch_id      as last_seen_batch,
    _source_file
from ranked
where recency = 1
