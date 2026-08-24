"""Tests for src/orchestration/spark_jobs.py (no Airflow, no Spark required)."""

import subprocess

import pytest

from src.orchestration import spark_jobs
from src.orchestration.spark_jobs import (
    build_spark_submit_command,
    monitored,
    reset_monitor_cache,
    run_silver_merge,
    run_spark_job,
)


class TestBuildSparkSubmitCommand:
    def test_bronze_includes_kafka_and_avro_packages(self):
        cmd = build_spark_submit_command("src.processing.bronze_streaming")
        joined = " ".join(cmd)
        assert "spark-sql-kafka-0-10_2.12" in joined
        assert "spark-avro_2.12" in joined
        assert "delta-core_2.12:2.4.0" in joined

    def test_silver_only_needs_delta(self):
        cmd = build_spark_submit_command("src.processing.bronze_to_silver")
        joined = " ".join(cmd)
        assert "delta-core_2.12:2.4.0" in joined
        assert "kafka" not in joined

    def test_python_worker_conf_always_set(self):
        cmd = build_spark_submit_command("src.maintenance.table_optimization")
        assert "--conf" in cmd
        assert any("spark.pyspark.python=" in part for part in cmd)


class TestRunSparkJob:
    def test_success_returns_stdout_tail(self, monkeypatch):
        captured = {}

        def fake_run(cmd, env=None, capture_output=True, text=True, timeout=None):
            captured["cmd"] = cmd
            captured["env"] = env
            return subprocess.CompletedProcess(cmd, 0, stdout="ok", stderr="")

        monkeypatch.setattr(spark_jobs.subprocess, "run", fake_run)
        out = run_spark_job(
            "src.processing.bronze_to_silver",
            extra_env={"SILVER_AVAILABLE_NOW": "true"},
        )
        assert out == "ok"
        assert captured["env"]["SILVER_AVAILABLE_NOW"] == "true"

    def test_failure_raises_called_process_error(self, monkeypatch):
        monkeypatch.setattr(
            spark_jobs.subprocess,
            "run",
            lambda *a, **kw: subprocess.CompletedProcess(a[0], 1, stdout="", stderr="boom"),
        )
        with pytest.raises(subprocess.CalledProcessError):
            run_spark_job("src.maintenance.table_optimization")


# ---------------------------------------------------------------------------
# monitored() wrapper — inject a fake monitor
# ---------------------------------------------------------------------------


class FakeMonitor:
    def __init__(self):
        self.events = []

    def start_run(self, pipeline_name, dag_id=None, **kw):
        self.events.append(("start", pipeline_name))
        return "run-1"

    def end_run(self, run_id, status, **kw):
        self.events.append(("end", run_id, status.value))


@pytest.fixture()
def fake_monitor(monkeypatch):
    mon = FakeMonitor()
    monkeypatch.setattr(spark_jobs, "_lazy_monitor", lambda: mon)
    reset_monitor_cache()
    yield mon
    reset_monitor_cache()


class TestMonitoredWrapper:
    def test_success_path_books_start_and_end(self, fake_monitor):
        @monitored("my_pipeline")
        def task():
            return 42

        assert task() == 42
        assert fake_monitor.events == [
            ("start", "my_pipeline"),
            ("end", "run-1", "success"),
        ]

    def test_failure_path_records_failed_status(self, fake_monitor):
        @monitored("bad_pipeline")
        def task():
            raise RuntimeError("explode")

        with pytest.raises(RuntimeError):
            task()
        assert fake_monitor.events[0] == ("start", "bad_pipeline")
        assert fake_monitor.events[1][0] == "end"
        assert fake_monitor.events[1][2] == "failed"


class TestTaskCallables:
    def test_run_silver_merge_sets_available_now(self, monkeypatch):
        seen = {}
        monkeypatch.setattr(
            spark_jobs,
            "run_spark_job",
            lambda module, extra_env=None, timeout_seconds=14400: seen.update(
                module=module, extra_env=extra_env or {}
            )
            or "",
        )
        run_silver_merge()
        assert seen["module"] == "src.processing.bronze_to_silver"
        assert seen["extra_env"]["SILVER_AVAILABLE_NOW"] == "true"
