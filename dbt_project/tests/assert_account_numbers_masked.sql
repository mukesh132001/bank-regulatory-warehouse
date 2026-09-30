-- Security check: every account number in the mart must look like ******1234.
-- Fails if a full account number ever leaks into the analyst-facing layer.

select account_id, account_number_masked
from {{ ref('mart_account_summary') }}
where account_number_masked !~ '^\*{6}[0-9]{4}$'
