"""
spark_jobs.py
-------------
Task callables for Airflow DAGs — deliberately Airflow-free.

Airflow DAG files stay thin (operator wiring only); all logic lives here so it
is importable and testable without an Airflow installation. Swap points for
production are documented per function: replace the subprocess spark-submit
with DataprocSubmitJobOperator / KubernetesPodOperator payloads, keeping these
functions as the local/dev path and as the single source of submit config.

Every callable is wrapped by `monitored()`, which books start/end runs into
PipelineMonitor (BigQuery) so DAG task executions feed the same anomaly
detection and SLA sweeps as Spark jobs. Without MONITOR_BQ_* env vars the
wrapper degrades to logging — orchestration never hard-depends on GCP.
"""

from __future__ import annotations

import functools
import logging
import os
import subprocess
from typing import Any, Callable, Dict, List, Optional

from src.utils.config import get_bool

logger = logging.getLogger(__name__)

# Delta jars for silver/maintenance; Kafka+Avro additionally for bronze
_DELTA_PACKAGE = "io.delta:delta-core_2.12:2.4.0"
_BRONZE_PACKAGES = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
        "org.apache.spark:spark-avro_2.12:3.4.1",
        _DELTA_PACKAGE,
    ]
)

_MODULES_REQUIRING = {
    "src.processing.bronze_streaming": _BRONZE_PACKAGES,
    "src.processing.bronze_to_silver": _DELTA_PACKAGE,
    "src.maintenance.table_optimization": _DELTA_PACKAGE,
    "src.validation.dq_runner": _DELTA_PACKAGE,
}


def build_spark_submit_command(
    module: str,
    python_bin: str = "python3",
    spark_submit_bin: str = "spark-submit",
    deploy_mode: str = "client",
) -> List[str]:
    """Compose the spark-submit command for one of our job modules."""
    packages = _MODULES_REQUIRING.get(module)
    cmd = [spark_submit_bin, "--deploy-mode", deploy_mode]
    if packages:
        cmd += ["--packages", packages]
    # Workers must run under a Python that matches the driver's pyspark.
    cmd += ["--conf", f"spark.pyspark.python={python_bin}"]
    cmd.append(module)
    return cmd


def run_spark_job(
    module: str,
    extra_env: Optional[Dict[str, str]] = None,
    timeout_seconds: int = 4 * 60 * 60,
) -> str:
    """
    Execute a job module via spark-submit; raises CalledProcessError on failure.

    Env passthrough: the current environment already carries KAFKA_* / GCS_*
    config; extra_env overlays per-invocation overrides (e.g. turning on
    SILVER_AVAILABLE_NOW=true for a backfill trigger).
    """
    env = {**os.environ, **(extra_env or {})}
    cmd = build_spark_submit_command(
        module=module,
        python_bin=env.get("PYSPARK_PYTHON", "python3"),
        spark_submit_bin=env.get("SPARK_SUBMIT_BIN", "spark-submit"),
    )
    logger.info(
        "Submitting %s | overlay_env_keys=%s", " ".join(cmd), sorted((extra_env or {}).keys())
    )
    result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=timeout_seconds)
    if result.returncode != 0:
        logger.error(
            "spark-submit failed | module=%s | rc=%d\nstderr tail:\n%s",
            module,
            result.returncode,
            result.stderr[-4000:],
        )
        raise subprocess.CalledProcessError(result.returncode, cmd)
    logger.info("spark-submit succeeded | module=%s", module)
    return result.stdout[-2000:] if result.stdout else ""


# ---------------------------------------------------------------------------
# Monitoring wrapper
# ---------------------------------------------------------------------------


def monitored(pipeline_name: str) -> Callable:
    """
    Decorator: wrap a task callable in PipelineMonitor start_run/end_run.
    No-op monitoring (log-only) when MONITOR_BQ_PROJECT is unset.
    """

    def deco(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            monitor = _lazy_monitor()
            run_id = (
                monitor.start_run(
                    pipeline_name=pipeline_name, dag_id=os.getenv("AIRFLOW_CTX_DAG_ID")
                )
                if monitor
                else None
            )
            try:
                result = fn(*args, **kwargs)
                if monitor and run_id:
                    monitor.end_run(run_id=run_id, status=_status_success())
                return result
            except Exception as exc:
                if monitor and run_id:
                    monitor.end_run(
                        run_id=run_id,
                        status=_status_failed(),
                        error_message=f"{type(exc).__name__}: {exc}"[:1000],
                    )
                raise

        return wrapper

    return deco


def _status_success():
    from src.observability.pipeline_monitor import PipelineStatus

    return PipelineStatus.SUCCESS


def _status_failed():
    from src.observability.pipeline_monitor import PipelineStatus

    return PipelineStatus.FAILED


_monitor_instance: Optional[Any] = None


def reset_monitor_cache() -> None:
    global _monitor_instance
    _monitor_instance = None


def _lazy_monitor():
    """
    Build PipelineMonitor once; returns None when unconfigured (dev/local).
    Stores False as the 'attempted-and-disabled' sentinel.
    """
    global _monitor_instance
    if _monitor_instance is not None:
        return _monitor_instance or None
    if not os.getenv("MONITOR_BQ_PROJECT") or not os.getenv("MONITOR_BQ_DATASET"):
        logger.debug("PipelineMonitor not configured — task metrics disabled")
        _monitor_instance = False  # sentinel: attempted-and-disabled
        return None
    try:
        from src.observability.pipeline_monitor import (
            BigQueryMonitoringConfig,
            MonitorConfig,
            PipelineMonitor,
        )

        _monitor_instance = PipelineMonitor(
            MonitorConfig(
                bq=BigQueryMonitoringConfig(
                    project=os.environ["MONITOR_BQ_PROJECT"],
                    dataset=os.environ["MONITOR_BQ_DATASET"],
                )
            )
        )
        return _monitor_instance
    except Exception as exc:
        logger.warning("Failed to init PipelineMonitor (%s) — continuing unmonitored", exc)
        _monitor_instance = False
        return None


# ---------------------------------------------------------------------------
# Concrete task callables (wired into airflow_dags/dags/*.py)
# ---------------------------------------------------------------------------


@monitored("silver_merge_available_now")
def run_silver_merge(**context: Any) -> str:
    """Hourly incremental Silver merge via availableNow trigger."""
    return run_spark_job("src.processing.bronze_to_silver", {"SILVER_AVAILABLE_NOW": "true"})


@monitored("bronze_backfill_available_now")
def run_bronze_backfill(**context: Any) -> str:
    """Backfill Bronze from Kafka retention window via availableNow trigger."""
    return run_spark_job("src.processing.bronze_streaming", {"BRONZE_AVAILABLE_NOW": "true"})


@monitored("table_maintenance")
def run_optimize_vacuum(**context: Any) -> str:
    """Nightly OPTIMIZE/Z-ORDER + VACUUM (dry-run unless explicitly enabled)."""
    return run_spark_job("src.maintenance.table_optimization")


@monitored("silver_dq_gate")
def run_silver_dq_gate(**context: Any) -> str:
    """Great Expectations-style gate over Silver tables; P1 failures fail the task."""
    return run_spark_job("src.validation.dq_runner")


@monitored("freshness_sla_sweep")
def check_freshness_slas(**context: Any) -> int:
    """Page/flag pipelines breaching freshness SLA. Returns breach count."""
    monitor = _lazy_monitor()
    if monitor is None:
        logger.info("Freshness sweep skipped — no monitor configured")
        return 0
    breaches = monitor.check_all_freshness_slas()
    return len(breaches)


@monitored("stale_run_reconciliation")
def reconcile_stale_runs(**context: Any) -> int:
    """Mark crashed 'running' rows as failed + fire incidents."""
    monitor = _lazy_monitor()
    if monitor is None:
        logger.info("Reconciliation skipped — no monitor configured")
        return 0
    reconciled = monitor.reconcile_stale_runs()
    return len(reconciled)


def dq_gate_enabled() -> bool:
    return get_bool("DQ_GATE_ENABLED", True)
