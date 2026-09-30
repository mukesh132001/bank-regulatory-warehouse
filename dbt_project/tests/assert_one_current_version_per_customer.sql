-- SCD Type 2 integrity: each customer must have exactly ONE current version.
-- A dbt singular test fails if this query returns any rows.

select
    customer_id,
    count(*) filter (where is_current) as current_versions
from {{ ref('dim_customer_scd2') }}
group by customer_id
having count(*) filter (where is_current) <> 1
