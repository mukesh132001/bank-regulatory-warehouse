-- SCD Type 3: current AND previous risk rating side by side as columns.
-- Only one level of history, but very easy to query.

with history as (
    select
        customer_id,
        risk_rating,
        last_updated_at,
        lag(risk_rating) over (partition by customer_id order by _batch_id) as prior_rating
    from {{ ref('stg_customers') }}
),

risk_changes as (
    select customer_id, prior_rating, last_updated_at as changed_at
    from history
    where prior_rating is not null
      and prior_rating <> risk_rating
),

latest_change as (
    select distinct on (customer_id)
        customer_id,
        prior_rating,
        changed_at
    from risk_changes
    order by customer_id, changed_at desc
)

select
    c.customer_id,
    c.risk_rating        as current_risk_rating,
    lc.prior_rating      as previous_risk_rating,
    lc.changed_at        as risk_rating_changed_at
from {{ ref('dim_customer') }} c
left join latest_change lc using (customer_id)
