-- One row per account with its balance on the reporting date. All PII masked.

with latest_balance as (
    select account_id, closing_balance
    from {{ ref('fct_account_daily_balance') }}
    where balance_date = '{{ var("reporting_date") }}'::date
)

select
    a.account_id,
    {{ mask_account_number('a.account_number') }}           as account_number_masked,
    a.account_type,
    a.open_date,
    c.customer_id,
    {{ mask_name('c.first_name', 'c.last_name') }}          as customer_name_masked,
    {{ mask_email('c.email') }}                             as email_masked,
    c.state,
    c.risk_rating,
    b.closing_balance                                       as current_balance
from {{ ref('dim_account') }} a
join {{ ref('dim_customer') }} c
    on c.customer_id = a.customer_id
left join latest_balance b
    on b.account_id = a.account_id
