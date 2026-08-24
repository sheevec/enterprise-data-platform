"""
edp_silver_hourly
-----------------
Hourly Silver pipeline: freshness SLA sweep → Bronze→Silver merge → DQ gate.

The Bronze streaming layer runs continuously OUTSIDE Airflow (always-on
spark-submit deployment); this DAG consumes what it landed. The DQ gate is a
hard promotion boundary — P1 failures stop downstream propagation and page
via the framework's PagerDuty routing.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.orchestration.spark_jobs import (
    check_freshness_slas,
    run_silver_dq_gate,
    run_silver_merge,
)

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # Airflow-native SLA: task must FINISH within 45min of scheduled time
    "sla": timedelta(minutes=45),
    "email_on_sla_miss": False,  # SLA misses surface via callback/PagerDuty integration
}

with DAG(
    dag_id="edp_silver_hourly",
    description="Freshness sweep -> Silver MERGE -> Data Quality gate",
    schedule_interval="0 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,  # hourly reprocessing handled by streaming checkpoints
    max_active_runs=1,  # never overlap merges against the same Delta target
    default_args=default_args,
    tags=["edp", "silver"],
) as dag:
    dag.doc_md = __doc__

    freshness = PythonOperator(
        task_id="freshness_sla_sweep",
        python_callable=check_freshness_slas,
    )

    silver_merge = PythonOperator(
        task_id="silver_merge",
        python_callable=run_silver_merge,
        execution_timeout=timedelta(minutes=40),
    )

    dq_gate = PythonOperator(
        task_id="silver_dq_gate",
        python_callable=run_silver_dq_gate,
        execution_timeout=timedelta(minutes=20),
    )

    freshness >> silver_merge >> dq_gate
