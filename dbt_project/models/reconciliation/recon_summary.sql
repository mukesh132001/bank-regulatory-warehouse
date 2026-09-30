{{ config(schema='reconciliation', materialized='table') }}

-- One row per reconciliation check: what an operations team reviews after every load.

with file_checks as (
    select
        'file_controls'                               as check_name,
        batch_id                                      as period,
        count(*)                                      as items_checked,
        count(*) filter (where status = 'FAIL')       as items_failed,
        null::numeric                                 as amount_difference
    from {{ ref('recon_file_controls') }}
    group by batch_id
),

txn_check as (
    select
        'transaction_count'                           as check_name,
        'all batches'                                 as period,
        (select count(distinct transaction_id) from {{ source('raw', 'transactions') }})
                                                      as items_checked,
        (select count(*) from {{ ref('fct_transactions') }})
          - (select count(distinct transaction_id) from {{ source('raw', 'transactions') }})
                                                      as items_failed,
        null::numeric                                 as amount_difference
),

balance_checks as (
    select
        'month_end_balances'                          as check_name,
        to_char(balance_date, 'YYYY-MM')              as period,
        count(*)                                      as items_checked,
        count(*) filter (where status <> 'MATCH')     as items_failed,
        sum(abs(difference))                          as amount_difference
    from {{ ref('recon_balances') }}
    group by 2
)

select
    *,
    case when items_failed = 0 then 'PASS' else 'FAIL' end as status
from (
    select * from file_checks
    union all
    select * from txn_check
    union all
    select * from balance_checks
) all_checks
order by check_name, period
