-- One row per transaction, linked to the customer VERSION valid at transaction time.

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
    t._source_row
from {{ ref('stg_transactions') }} t
join {{ ref('dim_account') }} a
    on a.account_id = t.account_id
left join {{ ref('dim_customer_scd2') }} v
    on  v.customer_id     = a.customer_id
    and t.transaction_ts >= v.valid_from
    and t.transaction_ts <  v.valid_to
