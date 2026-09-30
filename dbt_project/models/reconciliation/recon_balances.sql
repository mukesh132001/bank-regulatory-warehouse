{{ config(schema='reconciliation', materialized='table') }}

-- Check 3: does the warehouse's computed balance match the bank's OFFICIAL
-- month-end balance for every account?

select
    o.balance_date,
    o.account_id,
    o.balance                                   as official_balance,
    w.closing_balance                           as warehouse_balance,
    coalesce(w.closing_balance, 0) - o.balance  as difference,
    case
        when w.closing_balance is null   then 'MISSING_IN_WAREHOUSE'
        when w.closing_balance = o.balance then 'MATCH'
        else 'BREAK'
    end                                         as status
from {{ ref('stg_balances') }} o
left join {{ ref('fct_account_daily_balance') }} w
    on  w.account_id   = o.account_id
    and w.balance_date = o.balance_date
