"""
DAG integrity tests: run in CI so a broken DAG never reaches the scheduler.

    pip install "apache-airflow==3.3.2" apache-airflow-providers-standard pytest \
        --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt
    pytest tests/test_dag_integrity.py -v
"""

from datetime import timedelta
from pathlib import Path

import pytest

pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402

DAG_FOLDER = Path(__file__).resolve().parents[1] / "airflow" / "dags"
DAG_ID = "bank_regulatory_pipeline"


@pytest.fixture(scope="module")
def dagbag():
    # Airflow 3.3 removed include_examples; examples are off via AIRFLOW__CORE__LOAD_EXAMPLES.
    return DagBag(dag_folder=str(DAG_FOLDER))


@pytest.fixture(scope="module")
def dag(dagbag):
    return dagbag.dags[DAG_ID]


def test_dag_imports_without_errors(dagbag):
    assert dagbag.import_errors == {}
    assert DAG_ID in dagbag.dags


def test_pipeline_order(dag):
    def upstream(task_id):
        return {t.task_id for t in dag.get_task(task_id).upstream_list}

    assert upstream("land_in_s3") == {"generate_extracts"}
    assert upstream("load_raw") == {"land_in_s3"}
    assert upstream("dbt_staging") == {"load_raw"}
    assert upstream("dbt_test_staging") == {"dbt_staging"}
    assert upstream("spark_staging_parity") == {"dbt_test_staging"}
    assert upstream("dbt_warehouse") == {"dbt_test_staging"}
    assert upstream("dbt_marts") == {"dbt_warehouse"}
    assert upstream("reconciliation_check") == {"dbt_marts"}


def test_schedule_retries_and_alerting(dag):
    assert dag.schedule == "@daily"
    assert dag.max_active_runs == 1
    for task in dag.tasks:
        assert task.retries == 2, task.task_id
        assert task.retry_delay == timedelta(minutes=5), task.task_id
        assert task.on_failure_callback, task.task_id


def test_single_batch_rerun_param(dag):
    assert "batch" in dag.params
    for task_id in ("land_in_s3", "load_raw"):
        assert "params.batch" in dag.get_task(task_id).bash_command
