-- Typed account snapshots: one row per account per monthly batch.

with source as (
    select * from {{ source('raw', 'accounts') }}
)

select
    account_id,
    customer_id,
    account_number,
    upper(trim(account_type))              as account_type,
    currency,
    open_date::date                        as open_date,
    opening_balance::numeric(18, 2)        as opening_balance,
    opening_balance_date::date             as opening_balance_date,
    status,

    _batch_id,
    _source_file,
    _source_row,
    _loaded_at
from source
