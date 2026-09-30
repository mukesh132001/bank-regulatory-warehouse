-- One row per account with the latest values.
-- The full account number stays here (secure internal layer); marts will mask it.

with ranked as (
    select
        *,
        row_number() over (partition by account_id order by _batch_id desc) as recency
    from {{ ref('stg_accounts') }}
)

select
    account_id,
    customer_id,
    account_number,
    account_type,
    currency,
    open_date,
    opening_balance,
    opening_balance_date,
    status,
    _batch_id      as last_seen_batch,
    _source_file
from ranked
where recency = 1
