-- Official month-end balances reported by the core banking system.
-- Used as the "source of truth" in reconciliation.

with source as (
    select * from {{ source('raw', 'balances') }}
)

select
    account_id,
    balance_date::date                     as balance_date,
    balance::numeric(18, 2)                as balance,

    _batch_id,
    _source_file,
    _source_row,
    _loaded_at
from source
