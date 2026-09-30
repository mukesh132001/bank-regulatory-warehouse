-- "Active customers" per month under TWO definitions (see docs/kpi_definitions.md).
--   any_activity       : at least one transaction of any kind, including system interest
--   customer_initiated : at least one transaction the customer made themselves

with monthly as (
    select
        date_trunc('month', transaction_date)::date as month,
        customer_id,
        bool_or(is_customer_initiated)              as has_customer_txn
    from {{ ref('fct_transactions') }}
    group by 1, 2
)

select
    month,
    count(*)                                   as active_customers_any_activity,
    count(*) filter (where has_customer_txn)   as active_customers_customer_initiated,
    count(*) - count(*) filter (where has_customer_txn)
                                               as difference
from monthly
group by month
order by month
