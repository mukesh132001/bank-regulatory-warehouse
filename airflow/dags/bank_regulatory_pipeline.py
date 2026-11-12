"""
### Bank Regulatory Reporting Warehouse: daily pipeline

```
generate_extracts -> land_in_s3 -> load_raw -> dbt_staging -> dbt_test_staging
                                                   |-> spark_staging_parity
                                                   '-> dbt_warehouse -> dbt_marts -> reconciliation_check
```

**Every step is safe to rerun**

- the generator uses a fixed seed, so it rewrites identical extracts
- S3 uploads overwrite the same keys; a batch only counts once its control file lands
- the loader replaces a batch instead of appending it, in one transaction per batch
- dbt rebuilds models; `fct_transactions` merges new rows on `transaction_id`

**Re-run one month**: trigger the DAG with config `{"batch": "2026-03"}`.
Only that month is re-landed and re-loaded, dbt rebuilds everything downstream,
and the reconciliation gate has to pass again before the run turns green.

**Failures**: each task retries twice, 5 minutes apart. If it still fails, the
alert callback logs an error and, when `ALERT_WEBHOOK_URL` is set, posts it to
that (Slack-compatible) webhook.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

log = logging.getLogger(__name__)

PROJECT_DIR = os.environ.get("BANK_DW_HOME", "/opt/bank-dw")
PROJECT_VENV = os.environ.get("PROJECT_VENV", "/home/airflow/project-venv")

# Every task runs inside the project's own virtualenv, from the repo root.
IN_PROJECT = f"source {PROJECT_VENV}/bin/activate && cd {PROJECT_DIR}"
DBT = f"{IN_PROJECT}/dbt_project && dbt"

# Adds "--batch 2026-03" only when the run was triggered for a single month.
BATCH_ARG = "{% if params.batch %}--batch {{ params.batch }}{% endif %}"


def _send_alert(message: str) -> None:
    log.error(message)
    webhook = os.environ.get("ALERT_WEBHOOK_URL")
    if not webhook:
        return
    request = urllib.request.Request(
        webhook,
        data=json.dumps({"text": message}).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(request, timeout=10)
    except Exception:  # an alerting problem must never hide the real failure
        log.exception("Could not deliver the failure alert to ALERT_WEBHOOK_URL")


def alert_on_failure(context) -> None:
    """Called once a task has failed for good (after its retries)."""
    ti = context["task_instance"]
    try:
        log_url = ti.log_url
    except Exception:
        log_url = "see the Airflow UI"
    _send_alert(
        f":rotating_light: bank_regulatory_pipeline: task `{ti.task_id}` FAILED "
        f"after {ti.try_number} attempt(s) in run {ti.run_id}. "
        f"Error: {context.get('exception')}. Logs: {log_url}"
    )


def log_retry(context) -> None:
    ti = context["task_instance"]
    log.warning("Task %s failed on attempt %s, retrying in 5 minutes: %s",
                ti.task_id, ti.try_number, context.get("exception"))


with DAG(
    dag_id="bank_regulatory_pipeline",
    description="Extracts -> S3 -> raw -> dbt staging/warehouse/marts -> reconciliation",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": alert_on_failure,
        "on_retry_callback": log_retry,
    },
    params={
        "batch": Param(
            None,
            type=["null", "string"],
            pattern=r"^\d{4}-\d{2}$",
            description="Re-run a single monthly batch, e.g. 2026-03. Leave empty for all batches.",
        ),
    },
    tags=["bank", "regulatory-reporting", "dbt", "spark", "s3"],
    doc_md=__doc__,
) as dag:

    generate_extracts = BashOperator(
        task_id="generate_extracts",
        doc_md="Simulates the core banking system dropping its monthly extract files.",
        bash_command=f"{IN_PROJECT} && python data_generator/generate_data.py",
    )

    land_in_s3 = BashOperator(
        task_id="land_in_s3",
        doc_md="Uploads extracts to s3://<bucket>/raw/<entity>/batch=<YYYY-MM>/ (control file last).",
        bash_command=f"{IN_PROJECT} && python loader/land_to_s3.py --create-bucket {BATCH_ARG}",
    )

    load_raw = BashOperator(
        task_id="load_raw",
        doc_md="ACID, idempotent load from the S3 landing zone into the raw schema, with lineage.",
        bash_command=f"{IN_PROJECT} && python loader/load_raw.py --source s3 {BATCH_ARG}",
    )

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        doc_md="Type, clean and deduplicate (across all batches) in the staging layer.",
        bash_command=f"{DBT} run --select staging",
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        doc_md="Data-quality gate: unique transaction ids, accepted values, not-null keys.",
        bash_command=f"{DBT} test --select staging",
    )

    spark_staging_parity = BashOperator(
        task_id="spark_staging_parity",
        doc_md="PySpark rebuild of stg_transactions; fails if it disagrees with dbt.",
        bash_command=f"{IN_PROJECT} && python spark/stage_transactions.py --check-against-postgres",
    )

    dbt_warehouse = BashOperator(
        task_id="dbt_warehouse",
        doc_md="SCD 1/2/3 dimensions, incremental fct_transactions, daily balances + their tests.",
        bash_command=f"{DBT} build --select warehouse",
    )

    dbt_marts = BashOperator(
        task_id="dbt_marts",
        doc_md="Report-ready, PII-masked marts + masking test.",
        bash_command=f"{DBT} build --select marts",
    )

    reconciliation_check = BashOperator(
        task_id="reconciliation_check",
        doc_md="File controls, transaction counts and month-end balances vs the core banking system.",
        bash_command=f"{DBT} build --select reconciliation",
    )

    generate_extracts >> land_in_s3 >> load_raw >> dbt_staging >> dbt_test_staging
    dbt_test_staging >> [spark_staging_parity, dbt_warehouse]
    dbt_warehouse >> dbt_marts >> reconciliation_check
