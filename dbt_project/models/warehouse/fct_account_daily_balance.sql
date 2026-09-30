-- Daily closing balance for every account, every day (even days with no activity).
-- closing_balance = opening balance + running total of all signed transactions so far.

with accounts as (
    select account_id, customer_id, opening_balance, opening_balance_date
    from {{ ref('dim_account') }}
),

calendar as (
    select
        a.account_id,
        d::date as balance_date
    from accounts a
    cross join lateral generate_series(
        a.opening_balance_date::timestamp,
        '{{ var("reporting_date") }}'::timestamp,
        interval '1 day'
    ) as d
),

daily_movements as (
    select
        account_id,
        transaction_date,
        sum(signed_amount) as net_movement,
        count(*)           as transaction_count
    from {{ ref('fct_transactions') }}
    group by account_id, transaction_date
)

select
    c.account_id,
    a.customer_id,
    c.balance_date,
    coalesce(m.transaction_count, 0)   as transaction_count,
    coalesce(m.net_movement, 0)        as net_movement,
    a.opening_balance
      + sum(coalesce(m.net_movement, 0)) over (
            partition by c.account_id
            order by c.balance_date
            rows between unbounded preceding and current row
        )                              as closing_balance
from calendar c
join accounts a
    on a.account_id = c.account_id
left join daily_movements m
    on  m.account_id       = c.account_id
    and m.transaction_date = c.balance_date
