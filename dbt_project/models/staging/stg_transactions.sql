-- Typed, deduplicated transactions.

with source as (
    select * from {{ source('raw', 'transactions') }}
),

typed as (
    select
        transaction_id,
        account_id,
        transaction_ts::timestamp                          as transaction_ts,
        transaction_ts::date                               as transaction_date,
        transaction_type,
        direction,
        amount::numeric(18, 2)                             as amount,
        case when direction = 'CR' then amount::numeric(18, 2)
             else -amount::numeric(18, 2) end              as signed_amount,
        channel,
        description,
        -- interest is posted by the system, not by the customer
        (channel <> 'SYSTEM')                              as is_customer_initiated,

        _batch_id,
        _source_file,
        _source_row,
        _loaded_at
    from source
),

deduplicated as (
    -- The source system can deliver the same transaction more than once:
    -- twice in the same file, OR re-sent in a later month's file.
    -- Deduplicate on transaction_id ACROSS ALL BATCHES and keep the first copy
    -- received (earliest batch, then earliest row), so lineage points to the original.
    --
    -- INCIDENT-001: this previously partitioned by (transaction_id, _batch_id),
    -- which missed cross-batch re-sends and inflated balances.
    -- See docs/incident_001_balance_breaks.md
    select
        *,
        row_number() over (
            partition by transaction_id
            order by _batch_id, _source_row
        ) as copy_number
    from typed
)

select
    transaction_id,
    account_id,
    transaction_ts,
    transaction_date,
    transaction_type,
    direction,
    amount,
    signed_amount,
    channel,
    description,
    is_customer_initiated,
    _batch_id,
    _source_file,
    _source_row,
    _loaded_at
from deduplicated
where copy_number = 1
