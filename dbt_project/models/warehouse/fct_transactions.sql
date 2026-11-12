{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- One row per transaction, linked to the customer VERSION valid at transaction time.
--
-- Incremental: the first build (or --full-refresh) processes every transaction.
-- After that, each run only processes transactions whose raw row was loaded
-- since the last run (_loaded_at newer than anything already in this table) and
-- merges them on transaction_id. Re-loading a batch therefore refreshes exactly
-- that batch's transactions, and a re-sent duplicate is never inserted twice
-- (stg_transactions keeps the first copy received, with its original _loaded_at).
--
-- Safety net: recon_summary's transaction_count check compares this table with
-- raw on every run. If it ever drifts (e.g. a corrected file that removes rows),
-- assert_reconciliation_passes fails the pipeline until a --full-refresh fixes it.

select
    t.transaction_id,
    t.account_id,
    a.customer_id,
    v.customer_sk            as customer_version_sk,
    v.risk_rating            as risk_rating_at_transaction,
    t.transaction_ts,
    t.transaction_date,
    t.transaction_type,
    t.direction,
    t.amount,
    t.signed_amount,
    t.channel,
    t.description,
    t.is_customer_initiated,
    t._batch_id,
    t._source_file,
    t._source_row,
    t._loaded_at
from {{ ref('stg_transactions') }} t
join {{ ref('dim_account') }} a
    on a.account_id = t.account_id
left join {{ ref('dim_customer_scd2') }} v
    on  v.customer_id     = a.customer_id
    and t.transaction_ts >= v.valid_from
    and t.transaction_ts <  v.valid_to
{% if is_incremental() %}
where t._loaded_at > (
    select coalesce(max(_loaded_at), '1900-01-01'::timestamptz) from {{ this }}
)
{% endif %}
