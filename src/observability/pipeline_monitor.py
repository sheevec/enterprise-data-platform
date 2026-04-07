"""
pipeline_monitor.py
-------------------
PipelineMonitor: A production observability component for the Enterprise Data Platform
that tracks pipeline execution metrics, detects SLA violations, identifies volume
anomalies, and routes alerts to PagerDuty and Slack.

Features:
  - Record and persist pipeline run metrics (duration, rows, errors, status)
  - Detect data freshness SLA violations with configurable thresholds per pipeline
  - Compute data volume anomalies using z-score against a rolling historical average
  - Send PagerDuty P1 alerts for critical incidents (SLA breaches, pipeline failures)
  - Write all metrics to a BigQuery monitoring dataset for dashboarding and trend analysis
  - Thread-safe in-process metric store for low-latency same-session queries
"""

from __future__ import annotations

import json
import logging
import os
import statistics
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import requests
from google.cloud import bigquery

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Enums and constants
# ---------------------------------------------------------------------------


class PipelineStatus(str, Enum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    SLA_BREACHED = "sla_breached"


class IncidentSeverity(str, Enum):
    P1 = "P1"   # Critical — page on-call immediately
    P2 = "P2"   # High — Slack alert, ticket created
    P3 = "P3"   # Medium — Slack warning only
    P4 = "P4"   # Low — logged only


class AnomalyType(str, Enum):
    VOLUME_DROP = "volume_drop"
    VOLUME_SPIKE = "volume_spike"
    DURATION_SPIKE = "duration_spike"
    FRESHNESS_BREACH = "freshness_breach"
    PIPELINE_FAILURE = "pipeline_failure"
    NULL_RATE_INCREASE = "null_rate_increase"


# Z-score threshold beyond which a volume change is considered anomalous
DEFAULT_ANOMALY_Z_THRESHOLD = 3.0
# Minimum number of historical runs required before anomaly detection kicks in
MIN_HISTORY_FOR_ANOMALY = 7


# ---------------------------------------------------------------------------
# Configuration dataclasses
# ---------------------------------------------------------------------------


@dataclass
class SLAConfig:
    """
    Per-pipeline SLA definition.

    freshness_sla_minutes: Maximum allowed time since the pipeline's last successful
                           run before a freshness breach is declared.
    max_duration_minutes:  Maximum allowed wall-clock pipeline duration.
    min_rows:              Minimum rows expected in a successful run (0 = no check).
    """

    pipeline_name: str
    freshness_sla_minutes: int
    max_duration_minutes: Optional[int] = None
    min_rows: int = 0


@dataclass
class AlertRoutingConfig:
    """Alert destination configuration."""

    pagerduty_routing_key: Optional[str] = None
    slack_webhook_url: Optional[str] = None
    # Severity threshold above which PagerDuty is paged
    pagerduty_min_severity: IncidentSeverity = IncidentSeverity.P1
    # Severity threshold above which Slack is notified
    slack_min_severity: IncidentSeverity = IncidentSeverity.P2


@dataclass
class BigQueryMonitoringConfig:
    """Configuration for writing metrics to BigQuery."""

    project: str
    dataset: str
    runs_table: str = "pipeline_runs"
    anomalies_table: str = "pipeline_anomalies"
    incidents_table: str = "pipeline_incidents"

    def table_id(self, table: str) -> str:
        return f"{self.project}.{self.dataset}.{table}"


@dataclass
class MonitorConfig:
    """Top-level configuration for PipelineMonitor."""

    bq: BigQueryMonitoringConfig
    sla_configs: List[SLAConfig] = field(default_factory=list)
    alert: AlertRoutingConfig = field(default_factory=AlertRoutingConfig)
    anomaly_z_threshold: float = DEFAULT_ANOMALY_Z_THRESHOLD
    # Number of historical run rows to load for rolling stats
    rolling_window_runs: int = 30


# ---------------------------------------------------------------------------
# Metric data models
# ---------------------------------------------------------------------------


@dataclass
class PipelineRunMetrics:
    """Metrics captured for a single pipeline run."""

    run_id: str
    pipeline_name: str
    dag_id: Optional[str]
    started_at_utc: str
    ended_at_utc: Optional[str]
    duration_seconds: Optional[float]
    status: PipelineStatus
    rows_read: int
    rows_written: int
    rows_errored: int
    error_message: Optional[str]
    source_table: Optional[str]
    target_table: Optional[str]
    spark_job_id: Optional[str]
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_bq_row(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "pipeline_name": self.pipeline_name,
            "dag_id": self.dag_id,
            "started_at_utc": self.started_at_utc,
            "ended_at_utc": self.ended_at_utc,
            "duration_seconds": self.duration_seconds,
            "status": self.status.value,
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "rows_errored": self.rows_errored,
            "error_message": self.error_message,
            "source_table": self.source_table,
            "target_table": self.target_table,
            "spark_job_id": self.spark_job_id,
            "extra_json": json.dumps(self.extra),
        }


@dataclass
class AnomalyRecord:
    """Describes a detected anomaly."""

    anomaly_id: str
    pipeline_name: str
    run_id: str
    anomaly_type: AnomalyType
    detected_at_utc: str
    severity: IncidentSeverity
    observed_value: float
    expected_value: float
    z_score: Optional[float]
    description: str

    def to_bq_row(self) -> Dict[str, Any]:
        return {
            "anomaly_id": self.anomaly_id,
            "pipeline_name": self.pipeline_name,
            "run_id": self.run_id,
            "anomaly_type": self.anomaly_type.value,
            "detected_at_utc": self.detected_at_utc,
            "severity": self.severity.value,
            "observed_value": self.observed_value,
            "expected_value": self.expected_value,
            "z_score": self.z_score,
            "description": self.description,
        }


@dataclass
class IncidentRecord:
    """A fired incident with routing metadata."""

    incident_id: str
    pipeline_name: str
    run_id: str
    severity: IncidentSeverity
    fired_at_utc: str
    title: str
    description: str
    pagerduty_dedup_key: Optional[str]
    pagerduty_response_code: Optional[int]
    slack_response_code: Optional[int]

    def to_bq_row(self) -> Dict[str, Any]:
        return {
            "incident_id": self.incident_id,
            "pipeline_name": self.pipeline_name,
            "run_id": self.run_id,
            "severity": self.severity.value,
            "fired_at_utc": self.fired_at_utc,
            "title": self.title,
            "description": self.description,
            "pagerduty_dedup_key": self.pagerduty_dedup_key,
            "pagerduty_response_code": self.pagerduty_response_code,
            "slack_response_code": self.slack_response_code,
        }


# ---------------------------------------------------------------------------
# Rolling statistics helper
# ---------------------------------------------------------------------------


class RollingStats:
    """
    Computes mean, standard deviation, and z-scores for a numeric series.
    Used for volume and duration anomaly detection.
    """

    def __init__(self, values: List[float]) -> None:
        self._values = [v for v in values if v is not None]
        self._mean: Optional[float] = None
        self._stdev: Optional[float] = None

        if len(self._values) >= 2:
            self._mean = statistics.mean(self._values)
            self._stdev = statistics.stdev(self._values)
        elif len(self._values) == 1:
            self._mean = self._values[0]
            self._stdev = 0.0

    @property
    def mean(self) -> Optional[float]:
        return self._mean

    @property
    def stdev(self) -> Optional[float]:
        return self._stdev

    def z_score(self, value: float) -> Optional[float]:
        """Return the z-score for value relative to this distribution."""
        if self._mean is None or self._stdev is None:
            return None
        if self._stdev == 0:
            # All historical values are the same; any deviation is infinite
            return float("inf") if value != self._mean else 0.0
        return (value - self._mean) / self._stdev

    def is_anomalous(self, value: float, threshold: float) -> Tuple[bool, Optional[float]]:
        """Return (is_anomalous, z_score)."""
        z = self.z_score(value)
        if z is None:
            return False, None
        return abs(z) > threshold, z


# ---------------------------------------------------------------------------
# Main PipelineMonitor class
# ---------------------------------------------------------------------------


class PipelineMonitor:
    """
    Centralized observability hub for the Enterprise Data Platform.

    Usage:
        monitor = PipelineMonitor(config)

        # At pipeline start
        run_id = monitor.start_run(
            pipeline_name="payments_bronze_ingestion",
            dag_id="bronze_ingestion_dag",
            source_table="kafka://payments",
            target_table="gcs://bucket/bronze/payments",
        )

        # At pipeline end
        monitor.end_run(
            run_id=run_id,
            status=PipelineStatus.SUCCESS,
            rows_read=1_500_000,
            rows_written=1_498_432,
            rows_errored=1_568,
        )

        # Ad-hoc freshness SLA check (can be run on a schedule)
        monitor.check_freshness_sla("payments_bronze_ingestion")
    """

    def __init__(self, config: MonitorConfig) -> None:
        self._config = config
        self._bq_client = bigquery.Client(project=config.bq.project)
        self._sla_index: Dict[str, SLAConfig] = {
            s.pipeline_name: s for s in config.sla_configs
        }
        # In-memory store of active runs keyed by run_id
        self._active_runs: Dict[str, PipelineRunMetrics] = {}
        self._lock = threading.Lock()

        self._ensure_bq_tables()
        logger.info(
            "PipelineMonitor initialized | project=%s | dataset=%s | slas=%d",
            config.bq.project,
            config.bq.dataset,
            len(self._sla_index),
        )

    # ------------------------------------------------------------------
    # Public: run lifecycle
    # ------------------------------------------------------------------

    def start_run(
        self,
        pipeline_name: str,
        dag_id: Optional[str] = None,
        source_table: Optional[str] = None,
        target_table: Optional[str] = None,
        spark_job_id: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Register the start of a pipeline run.
        Returns a run_id that must be passed to end_run().
        """
        run_id = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc).isoformat()

        metrics = PipelineRunMetrics(
            run_id=run_id,
            pipeline_name=pipeline_name,
            dag_id=dag_id,
            started_at_utc=started_at,
            ended_at_utc=None,
            duration_seconds=None,
            status=PipelineStatus.RUNNING,
            rows_read=0,
            rows_written=0,
            rows_errored=0,
            error_message=None,
            source_table=source_table,
            target_table=target_table,
            spark_job_id=spark_job_id,
            extra=extra or {},
        )

        with self._lock:
            self._active_runs[run_id] = metrics

        logger.info(
            "Pipeline run started | run_id=%s | pipeline=%s",
            run_id,
            pipeline_name,
        )
        return run_id

    def end_run(
        self,
        run_id: str,
        status: PipelineStatus,
        rows_read: int = 0,
        rows_written: int = 0,
        rows_errored: int = 0,
        error_message: Optional[str] = None,
        extra_updates: Optional[Dict[str, Any]] = None,
    ) -> PipelineRunMetrics:
        """
        Finalize a pipeline run, persist metrics to BigQuery, and run checks.

        Args:
            run_id:         Run ID returned by start_run().
            status:         Final PipelineStatus.
            rows_read:      Total rows read from source.
            rows_written:   Rows successfully written to target.
            rows_errored:   Rows that failed processing.
            error_message:  Error detail if status == FAILED.
            extra_updates:  Additional key/value pairs to merge into the run extras.

        Returns:
            The completed PipelineRunMetrics.
        """
        with self._lock:
            if run_id not in self._active_runs:
                raise KeyError(f"Unknown run_id: {run_id}. Did you call start_run()?")
            metrics = self._active_runs.pop(run_id)

        ended_at = datetime.now(timezone.utc)
        started_at = datetime.fromisoformat(metrics.started_at_utc)
        duration_seconds = (ended_at - started_at).total_seconds()

        metrics.ended_at_utc = ended_at.isoformat()
        metrics.duration_seconds = duration_seconds
        metrics.status = status
        metrics.rows_read = rows_read
        metrics.rows_written = rows_written
        metrics.rows_errored = rows_errored
        metrics.error_message = error_message
        if extra_updates:
            metrics.extra.update(extra_updates)

        logger.info(
            "Pipeline run ended | run_id=%s | pipeline=%s | status=%s | "
            "rows_read=%d | rows_written=%d | duration=%.1fs",
            run_id,
            metrics.pipeline_name,
            status.value,
            rows_read,
            rows_written,
            duration_seconds,
        )

        # Persist to BigQuery
        self._write_run_to_bq(metrics)

        # Run post-run checks
        anomalies = self._run_post_run_checks(metrics)
        for anomaly in anomalies:
            self._write_anomaly_to_bq(anomaly)
            if anomaly.severity in (IncidentSeverity.P1, IncidentSeverity.P2):
                self._fire_incident(anomaly, metrics)

        return metrics

    def check_freshness_sla(self, pipeline_name: str) -> Optional[AnomalyRecord]:
        """
        Query BigQuery for the last successful run of pipeline_name and compare
        against the configured SLA. Returns an AnomalyRecord if SLA is breached,
        None otherwise.
        """
        sla = self._sla_index.get(pipeline_name)
        if not sla:
            logger.debug("No SLA configured for pipeline=%s", pipeline_name)
            return None

        last_success_at = self._query_last_success(pipeline_name)
        if last_success_at is None:
            logger.warning(
                "No successful runs found for pipeline=%s — cannot evaluate freshness SLA",
                pipeline_name,
            )
            return None

        now = datetime.now(timezone.utc)
        age_minutes = (now - last_success_at).total_seconds() / 60.0
        threshold_minutes = sla.freshness_sla_minutes

        if age_minutes <= threshold_minutes:
            logger.debug(
                "Freshness OK | pipeline=%s | age=%.1f min | sla=%d min",
                pipeline_name,
                age_minutes,
                threshold_minutes,
            )
            return None

        overage_minutes = age_minutes - threshold_minutes
        severity = (
            IncidentSeverity.P1 if overage_minutes > threshold_minutes * 0.5
            else IncidentSeverity.P2
        )

        anomaly = AnomalyRecord(
            anomaly_id=str(uuid.uuid4()),
            pipeline_name=pipeline_name,
            run_id="freshness-check",
            anomaly_type=AnomalyType.FRESHNESS_BREACH,
            detected_at_utc=now.isoformat(),
            severity=severity,
            observed_value=round(age_minutes, 2),
            expected_value=float(threshold_minutes),
            z_score=None,
            description=(
                f"Pipeline '{pipeline_name}' has not completed successfully in "
                f"{age_minutes:.1f} minutes. SLA is {threshold_minutes} minutes "
                f"(overage: {overage_minutes:.1f} min)."
            ),
        )

        logger.warning("Freshness SLA breached: %s", anomaly.description)
        self._write_anomaly_to_bq(anomaly)
        self._fire_incident(anomaly, metrics=None)
        return anomaly

    def check_all_freshness_slas(self) -> List[AnomalyRecord]:
        """Run freshness SLA checks for all configured pipelines."""
        breaches: List[AnomalyRecord] = []
        for pipeline_name in self._sla_index:
            result = self.check_freshness_sla(pipeline_name)
            if result:
                breaches.append(result)
        logger.info(
            "Freshness SLA sweep complete | checked=%d | breached=%d",
            len(self._sla_index),
            len(breaches),
        )
        return breaches

    def get_pipeline_health_summary(self, pipeline_name: str, hours: int = 24) -> Dict[str, Any]:
        """
        Return a summary of pipeline health over the last N hours.
        Queries BigQuery for runs, computes success rate, avg duration, and recent anomalies.
        """
        runs_table = self._config.bq.table_id(self._config.bq.runs_table)
        anomalies_table = self._config.bq.table_id(self._config.bq.anomalies_table)

        runs_query = f"""
            SELECT
                COUNT(*) AS total_runs,
                COUNTIF(status = 'success') AS successful_runs,
                COUNTIF(status = 'failed') AS failed_runs,
                AVG(duration_seconds) AS avg_duration_seconds,
                MAX(rows_written) AS max_rows_written,
                MIN(rows_written) AS min_rows_written
            FROM `{runs_table}`
            WHERE
                pipeline_name = @pipeline_name
                AND TIMESTAMP(started_at_utc) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
        """
        anomalies_query = f"""
            SELECT
                anomaly_type,
                severity,
                detected_at_utc,
                description
            FROM `{anomalies_table}`
            WHERE
                pipeline_name = @pipeline_name
                AND TIMESTAMP(detected_at_utc) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
            ORDER BY detected_at_utc DESC
            LIMIT 10
        """

        params = [bigquery.ScalarQueryParameter("pipeline_name", "STRING", pipeline_name)]

        try:
            runs_result = (
                self._bq_client.query(
                    runs_query, job_config=bigquery.QueryJobConfig(query_parameters=params)
                )
                .result()
                .to_dataframe()
                .to_dict(orient="records")
            )
            anomalies_result = (
                self._bq_client.query(
                    anomalies_query, job_config=bigquery.QueryJobConfig(query_parameters=params)
                )
                .result()
                .to_dataframe()
                .to_dict(orient="records")
            )
        except Exception as exc:
            logger.error("Failed to query pipeline health from BigQuery: %s", exc)
            return {"error": str(exc)}

        run_stats = runs_result[0] if runs_result else {}
        total = run_stats.get("total_runs", 0)
        success_rate = (
            run_stats.get("successful_runs", 0) / total if total > 0 else None
        )

        return {
            "pipeline_name": pipeline_name,
            "window_hours": hours,
            "total_runs": total,
            "successful_runs": run_stats.get("successful_runs"),
            "failed_runs": run_stats.get("failed_runs"),
            "success_rate": round(success_rate, 4) if success_rate is not None else None,
            "avg_duration_seconds": run_stats.get("avg_duration_seconds"),
            "max_rows_written": run_stats.get("max_rows_written"),
            "min_rows_written": run_stats.get("min_rows_written"),
            "recent_anomalies": anomalies_result,
        }

    # ------------------------------------------------------------------
    # Internal: post-run anomaly checks
    # ------------------------------------------------------------------

    def _run_post_run_checks(self, metrics: PipelineRunMetrics) -> List[AnomalyRecord]:
        """Run all anomaly checks after a pipeline run completes."""
        anomalies: List[AnomalyRecord] = []
        detected_at = datetime.now(timezone.utc).isoformat()

        # 1. Pipeline failure check
        if metrics.status == PipelineStatus.FAILED:
            anomalies.append(
                AnomalyRecord(
                    anomaly_id=str(uuid.uuid4()),
                    pipeline_name=metrics.pipeline_name,
                    run_id=metrics.run_id,
                    anomaly_type=AnomalyType.PIPELINE_FAILURE,
                    detected_at_utc=detected_at,
                    severity=IncidentSeverity.P1,
                    observed_value=1.0,
                    expected_value=0.0,
                    z_score=None,
                    description=(
                        f"Pipeline '{metrics.pipeline_name}' failed. "
                        f"Error: {metrics.error_message or 'no error message provided'}"
                    ),
                )
            )

        # 2. Volume anomaly detection
        volume_anomaly = self._detect_volume_anomaly(metrics, detected_at)
        if volume_anomaly:
            anomalies.append(volume_anomaly)

        # 3. Duration anomaly detection
        duration_anomaly = self._detect_duration_anomaly(metrics, detected_at)
        if duration_anomaly:
            anomalies.append(duration_anomaly)

        # 4. SLA: minimum rows check
        sla = self._sla_index.get(metrics.pipeline_name)
        if sla and sla.min_rows > 0 and metrics.rows_written < sla.min_rows:
            anomalies.append(
                AnomalyRecord(
                    anomaly_id=str(uuid.uuid4()),
                    pipeline_name=metrics.pipeline_name,
                    run_id=metrics.run_id,
                    anomaly_type=AnomalyType.VOLUME_DROP,
                    detected_at_utc=detected_at,
                    severity=IncidentSeverity.P2,
                    observed_value=float(metrics.rows_written),
                    expected_value=float(sla.min_rows),
                    z_score=None,
                    description=(
                        f"Pipeline '{metrics.pipeline_name}' wrote {metrics.rows_written:,} rows, "
                        f"below the configured minimum of {sla.min_rows:,}."
                    ),
                )
            )

        # 5. SLA: max duration check
        if (
            sla
            and sla.max_duration_minutes
            and metrics.duration_seconds is not None
            and metrics.duration_seconds / 60 > sla.max_duration_minutes
        ):
            actual_min = metrics.duration_seconds / 60
            anomalies.append(
                AnomalyRecord(
                    anomaly_id=str(uuid.uuid4()),
                    pipeline_name=metrics.pipeline_name,
                    run_id=metrics.run_id,
                    anomaly_type=AnomalyType.DURATION_SPIKE,
                    detected_at_utc=detected_at,
                    severity=IncidentSeverity.P2,
                    observed_value=round(actual_min, 2),
                    expected_value=float(sla.max_duration_minutes),
                    z_score=None,
                    description=(
                        f"Pipeline '{metrics.pipeline_name}' ran for {actual_min:.1f} minutes, "
                        f"exceeding the SLA maximum of {sla.max_duration_minutes} minutes."
                    ),
                )
            )

        return anomalies

    def _detect_volume_anomaly(
        self,
        metrics: PipelineRunMetrics,
        detected_at: str,
    ) -> Optional[AnomalyRecord]:
        """
        Fetch historical rows_written values for the pipeline and compute a z-score
        against the current run's rows_written.
        """
        if metrics.rows_written == 0 and metrics.status != PipelineStatus.SUCCESS:
            return None  # Skip volume check for failed runs (covered by failure check)

        history = self._query_historical_rows(metrics.pipeline_name)
        if len(history) < MIN_HISTORY_FOR_ANOMALY:
            logger.debug(
                "Insufficient history for volume anomaly detection | pipeline=%s | points=%d",
                metrics.pipeline_name,
                len(history),
            )
            return None

        stats = RollingStats(history)
        current = float(metrics.rows_written)
        is_anomalous, z_score = stats.is_anomalous(current, self._config.anomaly_z_threshold)

        if not is_anomalous:
            return None

        anomaly_type = (
            AnomalyType.VOLUME_DROP if z_score < 0 else AnomalyType.VOLUME_SPIKE
        )
        severity = IncidentSeverity.P1 if abs(z_score) > 5.0 else IncidentSeverity.P2

        description = (
            f"Volume anomaly detected for '{metrics.pipeline_name}': "
            f"rows_written={current:,.0f} "
            f"(rolling_mean={stats.mean:,.0f}, stdev={stats.stdev:,.0f}, z={z_score:.2f}). "
            f"Type: {anomaly_type.value}."
        )
        logger.warning(description)

        return AnomalyRecord(
            anomaly_id=str(uuid.uuid4()),
            pipeline_name=metrics.pipeline_name,
            run_id=metrics.run_id,
            anomaly_type=anomaly_type,
            detected_at_utc=detected_at,
            severity=severity,
            observed_value=current,
            expected_value=round(stats.mean, 2),
            z_score=round(z_score, 4),
            description=description,
        )

    def _detect_duration_anomaly(
        self,
        metrics: PipelineRunMetrics,
        detected_at: str,
    ) -> Optional[AnomalyRecord]:
        """Detect if the current run duration is anomalously long compared to history."""
        if metrics.duration_seconds is None:
            return None

        history = self._query_historical_durations(metrics.pipeline_name)
        if len(history) < MIN_HISTORY_FOR_ANOMALY:
            return None

        stats = RollingStats(history)
        current = metrics.duration_seconds
        is_anomalous, z_score = stats.is_anomalous(current, self._config.anomaly_z_threshold)

        if not is_anomalous or z_score <= 0:
            # Only flag duration spikes (slow runs), not unusually fast runs
            return None

        severity = IncidentSeverity.P1 if abs(z_score) > 5.0 else IncidentSeverity.P2

        description = (
            f"Duration anomaly for '{metrics.pipeline_name}': "
            f"duration={current:.1f}s "
            f"(rolling_mean={stats.mean:.1f}s, stdev={stats.stdev:.1f}s, z={z_score:.2f})."
        )
        logger.warning(description)

        return AnomalyRecord(
            anomaly_id=str(uuid.uuid4()),
            pipeline_name=metrics.pipeline_name,
            run_id=metrics.run_id,
            anomaly_type=AnomalyType.DURATION_SPIKE,
            detected_at_utc=detected_at,
            severity=severity,
            observed_value=round(current, 2),
            expected_value=round(stats.mean, 2),
            z_score=round(z_score, 4),
            description=description,
        )

    # ------------------------------------------------------------------
    # Internal: incident firing
    # ------------------------------------------------------------------

    def _fire_incident(
        self,
        anomaly: AnomalyRecord,
        metrics: Optional[PipelineRunMetrics],
    ) -> IncidentRecord:
        """Route an anomaly to PagerDuty and/or Slack based on severity."""
        incident_id = str(uuid.uuid4())
        fired_at = datetime.now(timezone.utc).isoformat()
        title = f"[{anomaly.severity.value}] {anomaly.anomaly_type.value} — {anomaly.pipeline_name}"
        alert_cfg = self._config.alert

        pd_dedup_key: Optional[str] = None
        pd_status_code: Optional[int] = None
        slack_status_code: Optional[int] = None

        # PagerDuty
        if (
            alert_cfg.pagerduty_routing_key
            and anomaly.severity == IncidentSeverity.P1
        ):
            pd_dedup_key, pd_status_code = self._page_pagerduty(anomaly, title, metrics)

        # Slack
        if (
            alert_cfg.slack_webhook_url
            and anomaly.severity.value <= alert_cfg.slack_min_severity.value
        ):
            slack_status_code = self._notify_slack(anomaly, title, metrics)

        incident = IncidentRecord(
            incident_id=incident_id,
            pipeline_name=anomaly.pipeline_name,
            run_id=anomaly.run_id,
            severity=anomaly.severity,
            fired_at_utc=fired_at,
            title=title,
            description=anomaly.description,
            pagerduty_dedup_key=pd_dedup_key,
            pagerduty_response_code=pd_status_code,
            slack_response_code=slack_status_code,
        )

        self._write_incident_to_bq(incident)
        return incident

    def _page_pagerduty(
        self,
        anomaly: AnomalyRecord,
        title: str,
        metrics: Optional[PipelineRunMetrics],
    ) -> Tuple[str, int]:
        """Trigger a PagerDuty incident. Returns (dedup_key, http_status_code)."""
        dedup_key = f"edp-{anomaly.pipeline_name}-{anomaly.anomaly_type.value}"
        custom_details: Dict[str, Any] = {
            "anomaly_id": anomaly.anomaly_id,
            "anomaly_type": anomaly.anomaly_type.value,
            "observed_value": anomaly.observed_value,
            "expected_value": anomaly.expected_value,
            "z_score": anomaly.z_score,
        }
        if metrics:
            custom_details.update(
                {
                    "run_id": metrics.run_id,
                    "rows_read": metrics.rows_read,
                    "rows_written": metrics.rows_written,
                    "duration_seconds": metrics.duration_seconds,
                    "status": metrics.status.value,
                }
            )

        payload = {
            "routing_key": self._config.alert.pagerduty_routing_key,
            "event_action": "trigger",
            "dedup_key": dedup_key,
            "payload": {
                "summary": title,
                "severity": "critical",
                "source": "enterprise-data-platform",
                "component": anomaly.pipeline_name,
                "group": "data-platform",
                "class": anomaly.anomaly_type.value,
                "custom_details": custom_details,
            },
        }

        try:
            response = requests.post(
                "https://events.pagerduty.com/v2/enqueue",
                json=payload,
                timeout=10,
            )
            logger.info(
                "PagerDuty alert fired | dedup_key=%s | status=%d",
                dedup_key,
                response.status_code,
            )
            return dedup_key, response.status_code
        except Exception as exc:
            logger.error("Failed to page PagerDuty: %s", exc)
            return dedup_key, -1

    def _notify_slack(
        self,
        anomaly: AnomalyRecord,
        title: str,
        metrics: Optional[PipelineRunMetrics],
    ) -> int:
        """Send a Slack notification. Returns the HTTP status code."""
        severity_emoji = {
            IncidentSeverity.P1: ":red_circle:",
            IncidentSeverity.P2: ":large_yellow_circle:",
            IncidentSeverity.P3: ":white_circle:",
            IncidentSeverity.P4: ":large_blue_circle:",
        }
        emoji = severity_emoji.get(anomaly.severity, ":question:")

        fields = [
            {"type": "mrkdwn", "text": f"*Pipeline:*\n`{anomaly.pipeline_name}`"},
            {"type": "mrkdwn", "text": f"*Anomaly Type:*\n{anomaly.anomaly_type.value}"},
            {
                "type": "mrkdwn",
                "text": f"*Observed:*\n{anomaly.observed_value:,.2f}",
            },
            {
                "type": "mrkdwn",
                "text": f"*Expected:*\n{anomaly.expected_value:,.2f}",
            },
        ]
        if anomaly.z_score is not None:
            fields.append({"type": "mrkdwn", "text": f"*Z-Score:*\n{anomaly.z_score:.2f}"})
        if metrics:
            fields.append({"type": "mrkdwn", "text": f"*Run ID:*\n`{metrics.run_id[:8]}...`"})

        payload = {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"{emoji} {title}",
                    },
                },
                {"type": "section", "fields": fields},
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Details:*\n{anomaly.description}",
                    },
                },
            ]
        }

        try:
            response = requests.post(
                self._config.alert.slack_webhook_url,
                json=payload,
                timeout=10,
            )
            logger.info(
                "Slack notification sent | pipeline=%s | status=%d",
                anomaly.pipeline_name,
                response.status_code,
            )
            return response.status_code
        except Exception as exc:
            logger.error("Failed to send Slack notification: %s", exc)
            return -1

    # ------------------------------------------------------------------
    # Internal: BigQuery queries
    # ------------------------------------------------------------------

    def _query_last_success(self, pipeline_name: str) -> Optional[datetime]:
        """Return the timestamp of the most recent successful run for pipeline_name."""
        table_id = self._config.bq.table_id(self._config.bq.runs_table)
        query = f"""
            SELECT ended_at_utc
            FROM `{table_id}`
            WHERE pipeline_name = @pipeline_name
              AND status = 'success'
            ORDER BY ended_at_utc DESC
            LIMIT 1
        """
        try:
            rows = list(
                self._bq_client.query(
                    query,
                    job_config=bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("pipeline_name", "STRING", pipeline_name)
                        ]
                    ),
                ).result()
            )
            if not rows:
                return None
            ts_str = rows[0]["ended_at_utc"]
            if isinstance(ts_str, datetime):
                return ts_str.replace(tzinfo=timezone.utc) if ts_str.tzinfo is None else ts_str
            return datetime.fromisoformat(str(ts_str)).replace(tzinfo=timezone.utc)
        except Exception as exc:
            logger.error("Failed to query last success for %s: %s", pipeline_name, exc)
            return None

    def _query_historical_rows(self, pipeline_name: str) -> List[float]:
        """Return recent rows_written values for anomaly baseline computation."""
        table_id = self._config.bq.table_id(self._config.bq.runs_table)
        limit = self._config.rolling_window_runs
        query = f"""
            SELECT rows_written
            FROM `{table_id}`
            WHERE pipeline_name = @pipeline_name
              AND status = 'success'
              AND rows_written IS NOT NULL
            ORDER BY started_at_utc DESC
            LIMIT {limit}
        """
        return self._query_float_column(query, pipeline_name, "rows_written")

    def _query_historical_durations(self, pipeline_name: str) -> List[float]:
        """Return recent duration_seconds values for anomaly baseline computation."""
        table_id = self._config.bq.table_id(self._config.bq.runs_table)
        limit = self._config.rolling_window_runs
        query = f"""
            SELECT duration_seconds
            FROM `{table_id}`
            WHERE pipeline_name = @pipeline_name
              AND status = 'success'
              AND duration_seconds IS NOT NULL
            ORDER BY started_at_utc DESC
            LIMIT {limit}
        """
        return self._query_float_column(query, pipeline_name, "duration_seconds")

    def _query_float_column(
        self, query: str, pipeline_name: str, column: str
    ) -> List[float]:
        try:
            rows = list(
                self._bq_client.query(
                    query,
                    job_config=bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("pipeline_name", "STRING", pipeline_name)
                        ]
                    ),
                ).result()
            )
            return [float(row[column]) for row in rows if row[column] is not None]
        except Exception as exc:
            logger.error(
                "Failed to query historical %s for %s: %s", column, pipeline_name, exc
            )
            return []

    # ------------------------------------------------------------------
    # Internal: BigQuery writes
    # ------------------------------------------------------------------

    def _write_run_to_bq(self, metrics: PipelineRunMetrics) -> None:
        table_id = self._config.bq.table_id(self._config.bq.runs_table)
        errors = self._bq_client.insert_rows_json(table_id, [metrics.to_bq_row()])
        if errors:
            logger.error("BQ insert errors for run %s: %s", metrics.run_id, errors)

    def _write_anomaly_to_bq(self, anomaly: AnomalyRecord) -> None:
        table_id = self._config.bq.table_id(self._config.bq.anomalies_table)
        errors = self._bq_client.insert_rows_json(table_id, [anomaly.to_bq_row()])
        if errors:
            logger.error("BQ insert errors for anomaly %s: %s", anomaly.anomaly_id, errors)

    def _write_incident_to_bq(self, incident: IncidentRecord) -> None:
        table_id = self._config.bq.table_id(self._config.bq.incidents_table)
        errors = self._bq_client.insert_rows_json(table_id, [incident.to_bq_row()])
        if errors:
            logger.error("BQ insert errors for incident %s: %s", incident.incident_id, errors)

    # ------------------------------------------------------------------
    # Internal: BigQuery table initialization
    # ------------------------------------------------------------------

    def _ensure_bq_tables(self) -> None:
        """Create BigQuery monitoring tables if they do not exist."""
        self._create_table_if_not_exists(
            self._config.bq.runs_table,
            self._runs_table_schema(),
            partition_field="started_at_utc",
        )
        self._create_table_if_not_exists(
            self._config.bq.anomalies_table,
            self._anomalies_table_schema(),
            partition_field="detected_at_utc",
        )
        self._create_table_if_not_exists(
            self._config.bq.incidents_table,
            self._incidents_table_schema(),
            partition_field="fired_at_utc",
        )

    def _create_table_if_not_exists(
        self,
        table_name: str,
        schema: List[bigquery.SchemaField],
        partition_field: str,
    ) -> None:
        table_id = self._config.bq.table_id(table_name)
        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        )
        try:
            self._bq_client.create_table(table, exists_ok=True)
            logger.info("BigQuery table ready: %s", table_id)
        except Exception as exc:
            logger.error("Failed to create table %s: %s", table_id, exc)

    @staticmethod
    def _runs_table_schema() -> List[bigquery.SchemaField]:
        F = bigquery.SchemaField
        return [
            F("run_id", "STRING", mode="REQUIRED"),
            F("pipeline_name", "STRING", mode="REQUIRED"),
            F("dag_id", "STRING", mode="NULLABLE"),
            F("started_at_utc", "TIMESTAMP", mode="REQUIRED"),
            F("ended_at_utc", "TIMESTAMP", mode="NULLABLE"),
            F("duration_seconds", "FLOAT64", mode="NULLABLE"),
            F("status", "STRING", mode="REQUIRED"),
            F("rows_read", "INT64", mode="REQUIRED"),
            F("rows_written", "INT64", mode="REQUIRED"),
            F("rows_errored", "INT64", mode="REQUIRED"),
            F("error_message", "STRING", mode="NULLABLE"),
            F("source_table", "STRING", mode="NULLABLE"),
            F("target_table", "STRING", mode="NULLABLE"),
            F("spark_job_id", "STRING", mode="NULLABLE"),
            F("extra_json", "STRING", mode="NULLABLE"),
        ]

    @staticmethod
    def _anomalies_table_schema() -> List[bigquery.SchemaField]:
        F = bigquery.SchemaField
        return [
            F("anomaly_id", "STRING", mode="REQUIRED"),
            F("pipeline_name", "STRING", mode="REQUIRED"),
            F("run_id", "STRING", mode="REQUIRED"),
            F("anomaly_type", "STRING", mode="REQUIRED"),
            F("detected_at_utc", "TIMESTAMP", mode="REQUIRED"),
            F("severity", "STRING", mode="REQUIRED"),
            F("observed_value", "FLOAT64", mode="REQUIRED"),
            F("expected_value", "FLOAT64", mode="REQUIRED"),
            F("z_score", "FLOAT64", mode="NULLABLE"),
            F("description", "STRING", mode="REQUIRED"),
        ]

    @staticmethod
    def _incidents_table_schema() -> List[bigquery.SchemaField]:
        F = bigquery.SchemaField
        return [
            F("incident_id", "STRING", mode="REQUIRED"),
            F("pipeline_name", "STRING", mode="REQUIRED"),
            F("run_id", "STRING", mode="REQUIRED"),
            F("severity", "STRING", mode="REQUIRED"),
            F("fired_at_utc", "TIMESTAMP", mode="REQUIRED"),
            F("title", "STRING", mode="REQUIRED"),
            F("description", "STRING", mode="REQUIRED"),
            F("pagerduty_dedup_key", "STRING", mode="NULLABLE"),
            F("pagerduty_response_code", "INT64", mode="NULLABLE"),
            F("slack_response_code", "INT64", mode="NULLABLE"),
        ]


# ---------------------------------------------------------------------------
# Factory helper
# ---------------------------------------------------------------------------


def build_monitor_from_env() -> PipelineMonitor:
    """
    Build a PipelineMonitor from environment variables.

    Required:
        MONITOR_BQ_PROJECT    — GCP project ID
        MONITOR_BQ_DATASET    — BigQuery dataset for monitoring tables
    Optional:
        MONITOR_PD_KEY        — PagerDuty routing key
        MONITOR_SLACK_URL     — Slack webhook URL
        MONITOR_SLA_JSON      — JSON list of SLAConfig dicts
        MONITOR_Z_THRESHOLD   — Z-score anomaly threshold (default: 3.0)
    """
    bq_project = os.environ["MONITOR_BQ_PROJECT"]
    bq_dataset = os.environ["MONITOR_BQ_DATASET"]

    sla_raw = os.getenv("MONITOR_SLA_JSON")
    sla_configs = [SLAConfig(**s) for s in json.loads(sla_raw)] if sla_raw else []

    config = MonitorConfig(
        bq=BigQueryMonitoringConfig(project=bq_project, dataset=bq_dataset),
        sla_configs=sla_configs,
        alert=AlertRoutingConfig(
            pagerduty_routing_key=os.getenv("MONITOR_PD_KEY"),
            slack_webhook_url=os.getenv("MONITOR_SLACK_URL"),
        ),
        anomaly_z_threshold=float(os.getenv("MONITOR_Z_THRESHOLD", str(DEFAULT_ANOMALY_Z_THRESHOLD))),
    )
    return PipelineMonitor(config)
