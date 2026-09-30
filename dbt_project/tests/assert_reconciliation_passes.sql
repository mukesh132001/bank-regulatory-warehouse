-- Fails the pipeline if ANY reconciliation check fails,
-- so unreconciled numbers never reach a regulatory report.

select *
from {{ ref('recon_summary') }}
where status = 'FAIL'
