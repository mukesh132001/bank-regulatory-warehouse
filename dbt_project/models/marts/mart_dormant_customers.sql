-- Customers with NO customer-initiated transaction in the 90 days before the reporting date.
-- System transactions (monthly interest) do not count as activity.

with last_activity as (
    select
        customer_id,
        max(transaction_date) filter (where is_customer_initiated) as last_customer_txn_date,
        max(transaction_date)                                      as last_any_txn_date
    from {{ ref('fct_transactions') }}
    group by customer_id
)

select
    c.customer_id,
    {{ mask_name('c.first_name', 'c.last_name') }}          as customer_name_masked,
    c.state,
    c.risk_rating,
    c.customer_since,
    la.last_customer_txn_date,
    la.last_any_txn_date,
    '{{ var("reporting_date") }}'::date
        - coalesce(la.last_customer_txn_date, c.customer_since) as days_inactive,
    -- TRUE = a naive "no transactions of any kind" rule would have MISSED this customer
    coalesce(la.last_any_txn_date >= '{{ var("reporting_date") }}'::date - 90, false)
                                                            as would_be_missed_by_naive_rule
from {{ ref('dim_customer') }} c
left join last_activity la
    on la.customer_id = c.customer_id
where coalesce(la.last_customer_txn_date, c.customer_since)
      < '{{ var("reporting_date") }}'::date - 90
