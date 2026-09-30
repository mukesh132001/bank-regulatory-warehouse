{{ config(schema='reconciliation', materialized='table') }}

-- Check 1: did every file load completely?
-- Compares raw row counts and amount totals with the source system's control file.

with actual as (
    select _batch_id as batch_id, 'customers.csv' as file_name,
           count(*) as actual_rows, null::numeric as actual_total
    from {{ source('raw', 'customers') }} group by 1
    union all
    select _batch_id, 'accounts.csv', count(*), null
    from {{ source('raw', 'accounts') }} group by 1
    union all
    select _batch_id, 'transactions.csv', count(*), sum(amount::numeric(18, 2))
    from {{ source('raw', 'transactions') }} group by 1
    union all
    select _batch_id, 'balances.csv', count(*), sum(balance::numeric(18, 2))
    from {{ source('raw', 'balances') }} group by 1
)

select
    c.batch_id,
    c.file_name,
    c.expected_row_count,
    a.actual_rows,
    c.expected_total,
    a.actual_total,
    case
        when a.actual_rows = c.expected_row_count
         and (c.expected_total is null or c.expected_total = a.actual_total)
        then 'PASS' else 'FAIL'
    end as status
from {{ source('audit', 'control_totals') }} c
left join actual a
    on a.batch_id = c.batch_id and a.file_name = c.file_name
