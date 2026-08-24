"""Unit tests for src/observability/pipeline_monitor.py (offline — no GCP required)."""

import json
from datetime import datetime, timezone

import pytest

from src.observability.pipeline_monitor import (
    BigQueryMonitoringConfig,
    IncidentSeverity,
    MonitorConfig,
    PipelineMonitor,
    PipelineRunMetrics,
    PipelineStatus,
    RollingStats,
)

# ---------------------------------------------------------------------------
# Rolling statistics (anomaly detection math)
# ---------------------------------------------------------------------------


class TestRollingStats:
    def test_mean_and_stdev(self):
        stats = RollingStats([1.0, 2.0, 3.0, 4.0, 5.0])
        assert stats.mean == pytest.approx(3.0)
        assert stats.stdev == pytest.approx(1.5811, rel=1e-3)

    def test_z_score(self):
        stats = RollingStats([1.0, 2.0, 3.0, 4.0, 5.0])
        assert stats.z_score(3.0) == pytest.approx(0.0)
        assert stats.z_score(6.0) > 1.5

    def test_zero_stdev_same_value(self):
        stats = RollingStats([5.0, 5.0, 5.0])
        assert stats.z_score(5.0) == 0.0

    def test_zero_stdev_different_value_is_anomalous(self):
        stats = RollingStats([5.0, 5.0, 5.0])
        is_anomalous, z = stats.is_anomalous(50.0, threshold=3.0)
        assert is_anomalous
        assert z == float("inf")

    def test_insufficient_history_returns_none_zscore(self):
        stats = RollingStats([])
        assert stats.z_score(1.0) is None
        assert stats.is_anomalous(1.0, 3.0) == (False, None)

    def test_none_values_filtered(self):
        stats = RollingStats([10.0, None, 20.0])
        assert stats.mean == pytest.approx(15.0)


# ---------------------------------------------------------------------------
# Severity model
# ---------------------------------------------------------------------------


class TestIncidentSeverity:
    def test_rank_ordering(self):
        assert IncidentSeverity.P4.rank < IncidentSeverity.P3.rank
        assert IncidentSeverity.P3.rank < IncidentSeverity.P2.rank
        assert IncidentSeverity.P2.rank < IncidentSeverity.P1.rank


# ---------------------------------------------------------------------------
# Config + models
# ---------------------------------------------------------------------------


class TestMonitorConfig:
    def test_stale_run_default(self):
        cfg = MonitorConfig(bq=BigQueryMonitoringConfig(project="p", dataset="d"))
        assert cfg.stale_run_minutes == 120
        assert cfg.anomaly_z_threshold == 3.0
        assert cfg.rolling_window_runs == 30


class TestModels:
    def test_run_metrics_bq_row_serializes_extra(self):
        metrics = PipelineRunMetrics(
            run_id="r-1",
            pipeline_name="payments",
            dag_id="dag-1",
            started_at_utc=datetime.now(timezone.utc).isoformat(),
            ended_at_utc=None,
            duration_seconds=None,
            status=PipelineStatus.RUNNING,
            rows_read=0,
            rows_written=0,
            rows_errored=0,
            error_message=None,
            source_table=None,
            target_table=None,
            spark_job_id=None,
            extra={"attempt": 2},
        )
        row = metrics.to_bq_row()
        assert row["status"] == "running"
        assert json.loads(row["extra_json"]) == {"attempt": 2}

    def test_lazy_bq_client_construction_makes_no_network_calls(self):
        """Regression guard: constructing a monitor must never touch GCP."""
        monitor = PipelineMonitor(
            MonitorConfig(
                bq=BigQueryMonitoringConfig(project="fake-proj", dataset="monitoring"),
                sla_configs=[
                    {
                        "pipeline_name": "payments_ingest",
                        "freshness_sla_minutes": 30,
                        "max_duration_minutes": 60,
                        "min_rows": 1000,
                    }
                ],
            )
        )
        assert monitor._bq_client is None  # lazy — nothing instantiated
        assert "payments_ingest" in monitor._sla_index
        assert monitor._sla_index["payments_ingest"].freshness_sla_minutes == 30

    def test_freshness_check_without_sla_is_noop(self):
        monitor = PipelineMonitor(
            MonitorConfig(bq=BigQueryMonitoringConfig(project="p", dataset="d"))
        )
        assert monitor.check_freshness_sla("unknown_pipeline") is None
