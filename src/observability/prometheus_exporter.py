"""
prometheus_exporter.py
----------------------
Prometheus instrumentation for the Bronze edge consumer (and reusable by any
platform process). Metrics scrape via /metrics on METRICS_PORT; disabled
entirely unless METRICS_ENABLED=true so local/dev runs pay zero overhead.

Design notes:
  - Counters for monotonic totals, Gauges for lag/depth snapshots.
  - Per-topic labels on message counters; per topic:partition on lag —
    cardinality is bounded by partition count, safe for label use.
  - The consumer calls the `record_*` helpers inside its hot paths; when
    disabled these are attribute-set-only no-ops (no registry lookups).
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class ConsumerPrometheusMetrics:
    """Prometheus metrics facade for DataPlatformConsumer."""

    def __init__(self, enabled: Optional[bool] = None, port: Optional[int] = None) -> None:
        self.enabled = (
            enabled
            if enabled is not None
            else os.getenv("METRICS_ENABLED", "false").lower() == "true"
        )
        self.port = port or int(os.getenv("METRICS_PORT", "9091"))
        self._server_thread: Any = None

        if not self.enabled:
            logger.info("Prometheus metrics disabled (METRICS_ENABLED=false)")
            return

        from prometheus_client import Counter, Gauge, start_http_server

        self.messages_consumed = Counter(
            "edp_consumer_messages_consumed_total",
            "Messages polled and accepted for Bronze",
            ["topic"],
        )
        self.deserialization_errors = Counter(
            "edp_consumer_deserialization_errors_total",
            "Avro/wire-format decode failures",
            ["topic"],
        )
        self.sent_to_dlq = Counter("edp_consumer_dlq_total", "Messages routed to DLQ", ["topic"])
        self.batches_written = Counter(
            "edp_consumer_batches_written_total", "Bronze Delta/Parquet batches", ["topic"]
        )
        self.gcs_write_errors = Counter(
            "edp_consumer_gcs_write_errors_total",
            "Failed GCS flushes (no offset commit)",
            ["topic"],
        )
        self.consumer_lag = Gauge(
            "edp_consumer_lag",
            "Per-partition consumer lag snapshot",
            ["topic", "partition"],
        )

        self._server_thread = start_http_server(self.port)
        logger.info("Prometheus metrics serving on :%d/metrics", self.port)

    # ------------------------------------------------------------------
    # Hot-path hooks (cheap no-ops when disabled)
    # ------------------------------------------------------------------

    def record_consumed(self, topic: str) -> None:
        if self.enabled:
            self.messages_consumed.labels(topic=topic).inc()

    def record_deserialization_error(self, topic: str) -> None:
        if self.enabled:
            self.deserialization_errors.labels(topic=topic).inc()

    def record_dlq(self, topic: str) -> None:
        if self.enabled:
            self.sent_to_dlq.labels(topic=topic).inc()

    def record_batch_written(self, topic: str) -> None:
        if self.enabled:
            self.batches_written.labels(topic=topic).inc()

    def record_gcs_error(self, topic: str) -> None:
        if self.enabled:
            self.gcs_write_errors.labels(topic=topic).inc()

    def record_lag_snapshot(self, lag_map: Dict[str, int]) -> None:
        """lag_map keys are 'topic:partition'."""
        if not self.enabled:
            return
        for key, lag in lag_map.items():
            topic, _, partition = key.rpartition(":")
            self.consumer_lag.labels(topic=topic, partition=partition).set(lag)


def build_from_env() -> ConsumerPrometheusMetrics:
    return ConsumerPrometheusMetrics()
