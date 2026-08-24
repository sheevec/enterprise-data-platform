"""
edp_maintenance_nightly
-----------------------
Nightly Delta housekeeping, deliberately inside the off-peak cost window
(README: cost-intensive jobs 12am-6am for sustained-use pricing):

    reconcile crashed runs -> OPTIMIZE/Z-ORDER -> VACUUM (dry-run default)

VACUUM runs in dry-run and only REPORTS deletable files; flipping to real
deletion is an explicit ops decision (VACUUM_DRY_RUN=false) guarded in the
runbook — never an automatic nightly side effect.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.orchestration.spark_jobs import reconcile_stale_runs, run_optimize_vacuum

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "sla": timedelta(hours=4),
}

with DAG(
    dag_id="edp_maintenance_nightly",
    description="Stale-run reconciliation -> OPTIMIZE/Z-ORDER -> VACUUM dry-run",
    schedule_interval="0 2 * * *",  # 02:00 UTC — off-peak window
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["edp", "maintenance"],
) as dag:
    dag.doc_md = __doc__

    reconcile = PythonOperator(
        task_id="reconcile_stale_runs",
        python_callable=reconcile_stale_runs,
    )

    optimize = PythonOperator(
        task_id="optimize_zorder_vacuum",
        python_callable=run_optimize_vacuum,
        execution_timeout=timedelta(hours=3),
    )

    reconcile >> optimize
