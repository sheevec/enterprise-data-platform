"""
edp_backfill_manual
-------------------
Operator-triggered backfill of an entity end-to-end:
Bronze availableNow replay from Kafka retention -> Silver availableNow merge.

Trigger with dag_run.conf overrides (all optional):
    {"bronze_topics_json": [...],       # restrict to specific topics
     "silver_jobs_json": [...],         # restrict to specific entities
     "starting_offsets": "{\"payments\":{\"0\":12345,\"1\":9876}}"}

Safety model (see airflow_dags/runbooks/BACKFILL_RUNBOOK.md):
  - Idempotent by construction: Delta txnAppId/txnVersion makes replays no-ops;
    SCD2 hash-merge skips unchanged rows. Clearing this task is always safe.
  - max_active_runs=1 + explicit ordering prevents concurrent writes to the
    same tables as the continuous streams' next micro-batch.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.orchestration.spark_jobs import run_bronze_backfill, run_silver_merge

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "sla": timedelta(hours=8),
}


def _overlay_from_conf(**context) -> dict:
    """Map dag_run.conf keys onto the jobs' env-var contract."""
    conf = (context.get("dag_run") and context["dag_run"].conf) or {}
    overlay = {}
    if conf.get("bronze_topics_json"):
        overlay["BRONZE_TOPICS_JSON"] = json.dumps(conf["bronze_topics_json"])
    if conf.get("silver_jobs_json"):
        overlay["SILVER_JOBS_JSON"] = json.dumps(conf["silver_jobs_json"])
    if conf.get("starting_offsets"):
        overlay["BRONZE_STARTING_OFFSETS_JSON"] = json.dumps(conf["starting_offsets"])
    return overlay


def bronze_backfill_task(**context):
    from src.orchestration.spark_jobs import run_spark_job

    env = _overlay_from_conf(**context)
    return run_spark_job("src.processing.bronze_streaming", {"BRONZE_AVAILABLE_NOW": "true", **env})


def silver_backfill_task(**context):
    from src.orchestration.spark_jobs import run_spark_job

    env = _overlay_from_conf(**context)
    return run_spark_job("src.processing.bronze_to_silver", {"SILVER_AVAILABLE_NOW": "true", **env})


with DAG(
    dag_id="edp_backfill_manual",
    description="Manual end-to-end backfill: Bronze availableNow -> Silver availableNow",
    schedule_interval=None,  # manual only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["edp", "backfill"],
) as dag:
    dag.doc_md = __doc__

    bronze = PythonOperator(task_id="bronze_backfill", python_callable=bronze_backfill_task)
    silver = PythonOperator(
        task_id="silver_backfill",
        python_callable=silver_backfill_task,
        execution_timeout=timedelta(hours=6),
    )

    bronze >> silver
