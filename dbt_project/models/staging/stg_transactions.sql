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
    -- The source system sometimes delivers the same transaction twice in a file.
    -- Keep the first copy received.
    select
        *,
        row_number() over (
            partition by transaction_id, _batch_id
            order by _source_row
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
