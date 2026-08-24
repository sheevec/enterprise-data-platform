"""
kafka_consumer.py
-----------------
DataPlatformConsumer: A production-grade Kafka consumer that reads Avro-serialized
messages from multiple topics, validates them against Confluent Schema Registry,
and writes partitioned Parquet files to the GCS Bronze layer.

Built on confluent-kafka (librdkafka) for high-throughput, C-speed networking.

Design notes:
  - One consumer instance per configured topic/group; horizontal scale comes from
    running multiple replicas (Kubernetes) so partitions spread across instances.
  - Cooperative-sticky assignment: incremental rebalances without stop-the-world.
  - Rebalance callbacks flush + commit the in-flight batch BEFORE partitions are
    revoked, preventing duplicate Bronze files after rebalances.
  - Offsets are committed only AFTER a confirmed GCS write (at-least-once).
    Downstream dedup keys on (_kafka_topic, _kafka_partition, _kafka_offset).
  - Consumer lag is sampled by a background thread every LAG_SAMPLE_INTERVAL_S
    seconds — never inline in the hot path.
  - Dead Letter Queue messages carry the ORIGINAL raw bytes as the value and all
    error metadata as Kafka headers (no hex-inflated JSON envelopes).
  - TLS/SASL is the default security posture; PLAINTEXT requires explicit opt-out
    and emits a loud warning on non-local bootstrap servers.
"""

from __future__ import annotations

import io
import json
import logging
import os
import signal
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer, TopicPartition
from google.cloud import storage

from src.ingestion.schema_registry import SchemaRegistryClient, SchemaRegistryConfig

__all__ = [
    "SchemaRegistryConfig",
    "SchemaRegistryClient",
    "TopicConfig",
    "ConsumerConfig",
    "ConsumerMetrics",
    "DataPlatformConsumer",
    "GCSBronzeWriter",
    "build_security_conf",
    "build_consumer_from_env",
]

logger = logging.getLogger(__name__)

# How often the background lag sampler runs (seconds). Deliberately decoupled
# from the consumption hot path: end_offsets()/committed() are broker RPCs and
# cost ~2 round trips per partition.
LAG_SAMPLE_INTERVAL_S = 30


# ---------------------------------------------------------------------------
# Configuration dataclasses
# ---------------------------------------------------------------------------


@dataclass
class TopicConfig:
    """Per-topic consumption settings."""

    topic: str
    group_id: str
    gcs_prefix: str  # e.g. "bronze/payments"
    dlq_topic: str  # Dead-letter queue topic name
    schema_subject: str  # Schema Registry subject name, e.g. "payments-value"
    batch_size: int = 5_000
    batch_timeout_ms: int = 5_000


@dataclass
class ConsumerConfig:
    """Top-level configuration for DataPlatformConsumer."""

    bootstrap_servers: List[str]
    gcs_bucket: str
    schema_registry: SchemaRegistryConfig
    topics: List[TopicConfig]
    # Kafka consumer settings
    auto_offset_reset: str = "earliest"
    session_timeout_ms: int = 30_000
    heartbeat_interval_ms: int = 10_000
    max_poll_interval_ms: int = 300_000
    fetch_max_wait_ms: int = 500
    # GCS write settings
    gcs_write_timeout_seconds: int = 60
    # DLQ producer settings
    dlq_retries: int = 3


# ---------------------------------------------------------------------------
# Metrics container
# ---------------------------------------------------------------------------


@dataclass
class ConsumerMetrics:
    """Tracks runtime metrics for the consumer instance."""

    messages_consumed: int = 0
    messages_written_to_gcs: int = 0
    messages_sent_to_dlq: int = 0
    deserialization_errors: int = 0
    gcs_write_errors: int = 0
    dlq_send_errors: int = 0
    batches_written: int = 0
    rebalances: int = 0
    # Per-partition lag snapshot {topic_partition_str: lag}
    consumer_lag: Dict[str, int] = field(default_factory=dict)
    lag_sampled_at_utc: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "messages_consumed": self.messages_consumed,
            "messages_written_to_gcs": self.messages_written_to_gcs,
            "messages_sent_to_dlq": self.messages_sent_to_dlq,
            "deserialization_errors": self.deserialization_errors,
            "gcs_write_errors": self.gcs_write_errors,
            "dlq_send_errors": self.dlq_send_errors,
            "batches_written": self.batches_written,
            "rebalances": self.rebalances,
            "consumer_lag": dict(self.consumer_lag),
            "lag_sampled_at_utc": self.lag_sampled_at_utc,
        }

    def log_snapshot(self) -> None:
        logger.info(
            "Consumer metrics snapshot: %s",
            json.dumps(self.to_dict(), indent=2, default=str),
        )


# ---------------------------------------------------------------------------
# Security configuration (TLS/SASL enforced by default)
# ---------------------------------------------------------------------------

_LOCAL_HOST_HINTS = ("localhost", "127.0.0.1", "::1", "kafka", "redpanda")


def build_security_conf(bootstrap_servers: List[str]) -> Dict[str, Any]:
    """
    Build the librdkafka security configuration block from environment variables.

    Env:
        KAFKA_SECURITY_PROTOCOL  — SASL_SSL (default) | SSL | SASL_PLAINTEXT | PLAINTEXT
        KAFKA_SASL_MECHANISM     — PLAIN (default) | SCRAM-SHA-256 | SCRAM-SHA-512 | OAUTHBEARER
        KAFKA_API_KEY / KAFKA_API_SECRET

    Raises ValueError if SASL selected without credentials.
    Emits a warning if PLAINTEXT/*PLAINTEXT is used against non-local brokers.
    """
    protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL").upper()
    conf: Dict[str, Any] = {"security.protocol": protocol}

    if protocol.startswith("SASL"):
        conf["sasl.mechanism"] = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
        username = os.getenv("KAFKA_API_KEY")
        password = os.getenv("KAFKA_API_SECRET")
        if not username or not password:
            raise ValueError(
                f"KAFKA_SECURITY_PROTOCOL={protocol} requires KAFKA_API_KEY "
                "and KAFKA_API_SECRET environment variables."
            )
        conf["sasl.username"] = username
        conf["sasl.password"] = password

    if protocol.endswith("PLAINTEXT"):
        non_local = [
            s
            for s in bootstrap_servers
            if not any(hint in s.split(":")[0] for hint in _LOCAL_HOST_HINTS)
        ]
        if non_local:
            logger.warning(
                "SECURITY WARNING: using %s against non-local brokers: %s. "
                "Credentials and payloads will traverse the network unencrypted.",
                protocol,
                non_local,
            )
    return conf


def merge_offset(
    pending_offsets: Dict[Tuple[str, int], int],
    topic: str,
    partition: int,
    offset: int,
) -> None:
    """Track the next-offset-to-commit per (topic, partition) for a pending batch."""
    key = (topic, partition)
    current = pending_offsets.get(key, -1)
    pending_offsets[key] = max(current, offset + 1)


# ---------------------------------------------------------------------------
# Schema Registry client lives in schema_registry.py (shared with the Spark
# Bronze streaming job). Re-exported here for backwards compatibility.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# GCS writer with partitioned paths
# ---------------------------------------------------------------------------


class GCSBronzeWriter:
    """
    Writes batches of Avro-decoded records as Snappy-compressed Parquet to GCS,
    partitioned by year/month/day/hour.

    Path pattern:
        gs://{bucket}/{prefix}/year={yyyy}/month={mm}/day={dd}/hour={hh}/{file_id}.parquet
    """

    def __init__(self, bucket_name: str, timeout_seconds: int = 60) -> None:
        self._client = storage.Client()
        self._bucket = self._client.bucket(bucket_name)
        self._timeout = timeout_seconds

    @staticmethod
    def _partition_path(prefix: str, ts: datetime) -> str:
        return (
            f"{prefix}/"
            f"year={ts.year:04d}/"
            f"month={ts.month:02d}/"
            f"day={ts.day:02d}/"
            f"hour={ts.hour:02d}"
        )

    def write_batch(
        self,
        records: List[Dict[str, Any]],
        prefix: str,
        partition_ts: datetime,
        file_id: str,
    ) -> str:
        """
        Serialize records to Parquet and upload to GCS.
        Returns the full GCS object path on success.
        """
        import pandas as pd

        partition_path = self._partition_path(prefix, partition_ts)
        object_name = f"{partition_path}/{file_id}.parquet"
        blob = self._bucket.blob(object_name)

        df = pd.DataFrame(records)
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine="pyarrow", compression="snappy", index=False)
        parquet_buffer.seek(0)

        blob.upload_from_file(
            parquet_buffer,
            content_type="application/octet-stream",
            timeout=self._timeout,
        )
        gcs_path = f"gs://{self._bucket.name}/{object_name}"
        logger.info(
            "Wrote %d records to %s (%.1f KB)",
            len(records),
            gcs_path,
            parquet_buffer.getbuffer().nbytes / 1024,
        )
        return gcs_path


# ---------------------------------------------------------------------------
# Per-topic consumer worker
# ---------------------------------------------------------------------------


class _TopicWorker:
    """
    Encapsulates one Kafka consumer (single topic subscription), its pending
    batch state, and its DLQ producer. Runs in its own thread.

    Rebalance callbacks execute inside poll() on this worker's own thread, so
    touching batch state from _on_revoke is thread-safe by construction.
    """

    def __init__(
        self,
        topic_cfg: TopicConfig,
        config: ConsumerConfig,
        security_conf: Dict[str, Any],
        schema_registry: SchemaRegistryClient,
        gcs_writer: GCSBronzeWriter,
        metrics: ConsumerMetrics,
        metrics_lock: threading.Lock,
        shutdown_event: threading.Event,
    ) -> None:
        self.topic_cfg = topic_cfg
        self._config = config
        self._security_conf = security_conf
        self._schema_registry = schema_registry
        self._gcs_writer = gcs_writer
        self._metrics = metrics
        self._metrics_lock = metrics_lock
        self._shutdown_event = shutdown_event

        self.batch: List[Dict[str, Any]] = []
        self.pending_offsets: Dict[Tuple[str, int], int] = {}
        self.batch_started_at = time.monotonic()

        self.consumer = self._build_consumer()
        self.dlq_producer = self._build_dlq_producer()

    # ------------------------------------------------------------------
    # Client factories
    # ------------------------------------------------------------------

    def _build_consumer(self) -> Consumer:
        cfg = self._config
        tcfg = self.topic_cfg
        conf = {
            "bootstrap.servers": ",".join(cfg.bootstrap_servers),
            "group.id": tcfg.group_id,
            "auto.offset.reset": cfg.auto_offset_reset,
            "enable.auto.commit": False,
            "session.timeout.ms": cfg.session_timeout_ms,
            "heartbeat.interval.ms": cfg.heartbeat_interval_ms,
            "max.poll.interval.ms": cfg.max_poll_interval_ms,
            "fetch.wait.max.ms": cfg.fetch_max_wait_ms,
            # Incremental, stop-the-world-free rebalancing
            "partition.assignment.strategy": "cooperative-sticky",
            "enable.partition.eof": False,
            "client.id": f"edp-bronze-{tcfg.topic}",
            **self._security_conf,
        }
        return Consumer(conf, on_assign=self._on_assign, on_revoke=self._on_revoke)

    def _build_dlq_producer(self) -> Producer:
        cfg = self._config
        conf = {
            "bootstrap.servers": ",".join(cfg.bootstrap_servers),
            "acks": "all",
            "retries": cfg.dlq_retries,
            "client.id": f"edp-dlq-{self.topic_cfg.topic}",
            **self._security_conf,
        }
        return Producer(conf)

    # ------------------------------------------------------------------
    # Rebalance callbacks (run on this worker's poll thread)
    # ------------------------------------------------------------------

    def _on_assign(self, consumer: Consumer, partitions: List[TopicPartition]) -> None:
        logger.info(
            "Partitions assigned | topic=%s | partitions=%s",
            self.topic_cfg.topic,
            sorted(p.partition for p in partitions),
        )

    def _on_revoke(self, consumer: Consumer, partitions: List[TopicPartition]) -> None:
        """
        Flush and commit everything in flight BEFORE ownership is lost.
        Prevents duplicate Bronze files when partitions move between replicas.
        """
        revoked = sorted(p.partition for p in partitions)
        logger.info(
            "Partitions revoking — flushing in-flight batch first | topic=%s | partitions=%s",
            self.topic_cfg.topic,
            revoked,
        )
        with self._metrics_lock:
            self._metrics.rebalances += 1
        try:
            self._flush(consumer)
        except Exception as exc:
            # Do not propagate into librdkafka's callback machinery: uncommitted
            # offsets simply get reprocessed by whoever owns these partitions next.
            logger.error(
                "Flush during revoke failed — affected records will be reprocessed "
                "(at-least-once) | topic=%s | error=%s",
                self.topic_cfg.topic,
                exc,
                exc_info=True,
            )
            self.batch.clear()
            self.pending_offsets.clear()

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Poll loop: consume → deserialize → batch → write GCS → commit."""
        tcfg = self.topic_cfg
        self.consumer.subscribe([tcfg.topic])
        logger.info(
            "Topic consumer ready | topic=%s | group=%s",
            tcfg.topic,
            tcfg.group_id,
        )

        try:
            while not self._shutdown_event.is_set():
                msg = self.consumer.poll(timeout=0.5)

                if msg is None:
                    self._maybe_flush()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._ALL_BROKERS_DOWN:
                        logger.error("All brokers down for topic=%s", tcfg.topic)
                    else:
                        logger.error("Kafka error on topic=%s: %s", tcfg.topic, msg.error())
                    continue

                self._handle_message(msg)
                self._maybe_flush()

            # Final flush on graceful shutdown
            if self.batch:
                logger.info(
                    "Flushing %d remaining records on shutdown for topic=%s",
                    len(self.batch),
                    tcfg.topic,
                )
                self._flush(self.consumer)

        except Exception as exc:
            logger.error(
                "Unhandled exception in consumer thread for topic=%s: %s",
                tcfg.topic,
                exc,
                exc_info=True,
            )
        finally:
            try:
                self.consumer.close()
            except Exception as close_exc:
                logger.warning("Error closing consumer: %s", close_exc)
            remaining = self.dlq_producer.flush(timeout=10)
            if remaining:
                logger.error("%d DLQ messages never delivered", remaining)
            logger.info("Consumer thread exiting for topic=%s", tcfg.topic)

    # ------------------------------------------------------------------
    # Message handling
    # ------------------------------------------------------------------

    def _handle_message(self, msg: Message) -> None:
        try:
            decoded = self._deserialize(msg.value())
            decoded["_kafka_topic"] = msg.topic()
            decoded["_kafka_partition"] = msg.partition()
            decoded["_kafka_offset"] = msg.offset()
            decoded["_kafka_timestamp_ms"] = msg.timestamp()[1]
            decoded["_ingested_utc"] = datetime.now(timezone.utc).isoformat()
            self.batch.append(decoded)
            merge_offset(self.pending_offsets, msg.topic(), msg.partition(), msg.offset())

            with self._metrics_lock:
                self._metrics.messages_consumed += 1

        except Exception as deser_exc:
            logger.warning(
                "Deserialization failed | topic=%s | partition=%d | offset=%d | error=%s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
                deser_exc,
            )
            with self._metrics_lock:
                self._metrics.deserialization_errors += 1
            self._send_to_dlq(msg, deser_exc)
            # The failed message itself is skipped; its offset advances with the
            # next successful message from this partition (or on flush).
            merge_offset(self.pending_offsets, msg.topic(), msg.partition(), msg.offset())

    def _maybe_flush(self) -> None:
        elapsed_ms = (time.monotonic() - self.batch_started_at) * 1000
        should_flush = len(self.batch) >= self.topic_cfg.batch_size or (
            elapsed_ms >= self.topic_cfg.batch_timeout_ms and len(self.batch) > 0
        )
        if should_flush:
            self._flush(self.consumer)

    def _deserialize(self, raw_bytes: Optional[bytes]) -> Dict[str, Any]:
        return self._schema_registry.decode_message(raw_bytes or b"")

    # ------------------------------------------------------------------
    # Batch flush + offset commit
    # ------------------------------------------------------------------

    def _flush(self, consumer: Consumer) -> None:
        """Write the pending batch to GCS, then commit offsets atomically."""
        if not self.batch:
            return

        partition_ts = datetime.now(timezone.utc)
        file_id = f"{partition_ts.strftime('%Y%m%dT%H%M%S%f')}-{uuid.uuid4().hex[:8]}"

        try:
            self._gcs_writer.write_batch(
                records=self.batch,
                prefix=self.topic_cfg.gcs_prefix,
                partition_ts=partition_ts,
                file_id=file_id,
            )
        except Exception as gcs_exc:
            with self._metrics_lock:
                self._metrics.gcs_write_errors += 1
            logger.error(
                "GCS write failed | topic=%s | records=%d | error=%s",
                self.topic_cfg.topic,
                len(self.batch),
                gcs_exc,
                exc_info=True,
            )
            # Do NOT commit — records will be re-consumed (at-least-once).
            # Drop the local batch to avoid unbounded memory growth; offsets
            # stay uncommitted so the next owner reprocesses these records.
            self.batch.clear()
            self.pending_offsets.clear()
            self.batch_started_at = time.monotonic()
            return

        offsets = [
            TopicPartition(topic=t, partition=p, offset=next_off)
            for (t, p), next_off in self.pending_offsets.items()
        ]
        try:
            self.consumer.commit(offsets=offsets, asynchronous=False)
        except KafkaException as commit_exc:
            with self._metrics_lock:
                self._metrics.gcs_write_errors += 1
            logger.error(
                "Offset commit failed after successful GCS write — duplicates possible "
                "| topic=%s | error=%s",
                self.topic_cfg.topic,
                commit_exc,
            )

        with self._metrics_lock:
            self._metrics.messages_written_to_gcs += len(self.batch)
            self._metrics.batches_written += 1

        logger.debug(
            "Flushed batch | topic=%s | records=%d | file_id=%s",
            self.topic_cfg.topic,
            len(self.batch),
            file_id,
        )
        self.batch.clear()
        self.pending_offsets.clear()
        self.batch_started_at = time.monotonic()

    # ------------------------------------------------------------------
    # DLQ
    # ------------------------------------------------------------------

    def _send_to_dlq(self, msg: Message, error: Exception) -> None:
        """
        Publish the failed message to the DLQ topic.

        The ORIGINAL raw payload is preserved as the record value (byte-for-byte,
        replayable); all error context travels as Kafka headers. This avoids the
        ~2x size blowup of hex-encoded JSON envelopes.
        """
        headers: List[Tuple[str, Optional[bytes]]] = [
            ("original_topic", msg.topic().encode()),
            ("original_partition", str(msg.partition()).encode()),
            ("original_offset", str(msg.offset()).encode()),
            ("original_timestamp_ms", str(msg.timestamp()[1]).encode()),
            ("error_type", type(error).__name__.encode()),
            ("error_message", str(error)[:4096].encode()),
            ("failed_at_utc", datetime.now(timezone.utc).isoformat().encode()),
            ("source_consumer_group", self.topic_cfg.group_id.encode()),
        ]

        try:
            self.dlq_producer.produce(
                self.topic_cfg.dlq_topic,
                value=msg.value() or b"",
                key=msg.key(),
                headers=headers,
                on_delivery=self._dlq_delivery_report,
            )
            self.dlq_producer.poll(0)  # serve delivery-report callbacks
            with self._metrics_lock:
                self._metrics.messages_sent_to_dlq += 1
        except BufferError:
            logger.warning("DLQ producer queue full — flushing and retrying once")
            self.dlq_producer.flush(timeout=10)
            try:
                self.dlq_producer.produce(
                    self.topic_cfg.dlq_topic,
                    value=msg.value() or b"",
                    key=msg.key(),
                    headers=headers,
                    on_delivery=self._dlq_delivery_report,
                )
                with self._metrics_lock:
                    self._metrics.messages_sent_to_dlq += 1
            except Exception as retry_exc:
                with self._metrics_lock:
                    self._metrics.dlq_send_errors += 1
                logger.error("DLQ retry failed: %s", retry_exc)
        except Exception as dlq_exc:
            with self._metrics_lock:
                self._metrics.dlq_send_errors += 1
            logger.error(
                "Failed to send message to DLQ | dlq_topic=%s | error=%s",
                self.topic_cfg.dlq_topic,
                dlq_exc,
                exc_info=True,
            )

    @staticmethod
    def _dlq_delivery_report(err: Optional[KafkaError], msg: Optional[Message]) -> None:
        if err is not None:
            logger.error("DLQ delivery failed: %s", err)

    # ------------------------------------------------------------------
    # Lag sampling (called ONLY from the background sampler thread)
    # ------------------------------------------------------------------

    def update_lag(self) -> Dict[str, int]:
        """Compute per-partition lag via broker queries. Costly — run periodically."""
        lag_map: Dict[str, int] = {}
        try:
            assigned = self.consumer.assignment()
            if not assigned:
                return lag_map
            positions = {
                (tp.topic, tp.partition): tp.offset for tp in self.consumer.position(assigned)
            }
            committed_list = self.consumer.committed(list(assigned), timeout=5)
            committed = {(tp.topic, tp.partition): tp.offset for tp in committed_list}

            for tp in assigned:
                end = committed.get((tp.topic, tp.partition))
                pos = positions.get((tp.topic, tp.partition))
                if end is None or end < 0 or pos is None or pos < 0:
                    continue
                lag_map[f"{tp.topic}:{tp.partition}"] = max(0, end - pos)

            with self._metrics_lock:
                self._metrics.consumer_lag = lag_map
                self._metrics.lag_sampled_at_utc = datetime.now(timezone.utc).isoformat()
        except Exception as lag_exc:
            logger.debug("Could not compute consumer lag: %s", lag_exc)
        return lag_map


# ---------------------------------------------------------------------------
# Main consumer class
# ---------------------------------------------------------------------------


class DataPlatformConsumer:
    """
    Production Kafka consumer for the Enterprise Data Platform Bronze layer.

    Lifecycle:
        consumer = DataPlatformConsumer(config)
        consumer.start()                     # begins consuming in background threads
        ...
        consumer.stop()                      # graceful shutdown
        metrics = consumer.get_metrics()

    Each TopicConfig results in an independent consumer thread consuming that
    topic, deserializing Avro payloads via Schema Registry, batching records,
    writing Parquet files to GCS, and routing failures to a DLQ topic.
    """

    def __init__(self, config: ConsumerConfig) -> None:
        self._config = config
        self._security_conf = build_security_conf(config.bootstrap_servers)
        self._schema_registry = SchemaRegistryClient(config.schema_registry)
        self._gcs_writer = GCSBronzeWriter(
            config.gcs_bucket,
            timeout_seconds=config.gcs_write_timeout_seconds,
        )
        self._metrics = ConsumerMetrics()
        self._metrics_lock = threading.Lock()
        self._running = False
        self._workers: List[_TopicWorker] = []
        self._threads: List[threading.Thread] = []
        self._shutdown_event = threading.Event()

        logger.info(
            "DataPlatformConsumer initialized | bucket=%s | topics=%s | security=%s",
            config.gcs_bucket,
            [t.topic for t in config.topics],
            self._security_conf.get("security.protocol"),
        )

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start consumer threads for all configured topics."""
        if self._running:
            logger.warning("Consumer is already running.")
            return

        self._running = True
        self._shutdown_event.clear()

        for topic_cfg in self._config.topics:
            worker = _TopicWorker(
                topic_cfg=topic_cfg,
                config=self._config,
                security_conf=self._security_conf,
                schema_registry=self._schema_registry,
                gcs_writer=self._gcs_writer,
                metrics=self._metrics,
                metrics_lock=self._metrics_lock,
                shutdown_event=self._shutdown_event,
            )
            self._workers.append(worker)
            t = threading.Thread(target=worker.run, name=f"consumer-{topic_cfg.topic}", daemon=True)
            t.start()
            self._threads.append(t)
            logger.info("Started consumer thread for topic=%s", topic_cfg.topic)

        metrics_thread = threading.Thread(
            target=self._metrics_reporter, name="metrics-reporter", daemon=True
        )
        metrics_thread.start()
        self._threads.append(metrics_thread)

        lag_thread = threading.Thread(target=self._lag_sampler, name="lag-sampler", daemon=True)
        lag_thread.start()
        self._threads.append(lag_thread)

    def stop(self, timeout_seconds: int = 30) -> None:
        """Signal all consumer threads to stop and wait for them to finish."""
        logger.info("Initiating graceful shutdown...")
        self._running = False
        self._shutdown_event.set()

        for t in self._threads:
            t.join(timeout=timeout_seconds)
            if t.is_alive():
                logger.warning("Thread %s did not stop within %ds", t.name, timeout_seconds)

        with self._metrics_lock:
            self._metrics.log_snapshot()
        logger.info("DataPlatformConsumer stopped.")

    def get_metrics(self) -> Dict[str, Any]:
        """Return a snapshot of current consumer metrics."""
        with self._metrics_lock:
            return self._metrics.to_dict()

    def is_running(self) -> bool:
        return self._running

    def install_signal_handlers(self) -> None:
        """
        Register OS signal handlers for graceful shutdown.

        Call explicitly from the main thread of a standalone process (never from
        constructors — signal.signal() raises outside the main thread).
        """
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    # ------------------------------------------------------------------
    # Background reporters
    # ------------------------------------------------------------------

    def _metrics_reporter(self) -> None:
        """Periodically log a metrics snapshot every 60 seconds."""
        while not self._shutdown_event.wait(timeout=60):
            with self._metrics_lock:
                self._metrics.log_snapshot()

    def _lag_sampler(self) -> None:
        """Sample consumer lag on a fixed interval — NEVER inline per-message."""
        while not self._shutdown_event.wait(timeout=LAG_SAMPLE_INTERVAL_S):
            for worker in list(self._workers):
                worker.update_lag()

    # ------------------------------------------------------------------
    # Signal handling
    # ------------------------------------------------------------------

    def _handle_signal(self, signum: int, frame: Any) -> None:
        logger.info("Received signal %d — initiating graceful shutdown", signum)
        self.stop()


# ---------------------------------------------------------------------------
# Entrypoint helper for local testing / CLI usage
# ---------------------------------------------------------------------------


def build_consumer_from_env() -> DataPlatformConsumer:
    """
    Build a DataPlatformConsumer from environment variables.
    Useful for containerized deployments where config is injected via env.

    Required environment variables:
        KAFKA_BOOTSTRAP_SERVERS   — comma-separated list, e.g. "broker1:9092,broker2:9092"
        KAFKA_SCHEMA_REGISTRY_URL — e.g. "https://schema-registry.example.com"
        KAFKA_SCHEMA_REGISTRY_AUTH — "key:secret" for Confluent Cloud (optional)
        GCS_BUCKET_NAME           — target GCS bucket for Bronze layer
        KAFKA_TOPIC_CONFIG_JSON   — JSON list of TopicConfig dicts (see TopicConfig dataclass)

    Optional:
        CONSUMER_BATCH_SIZE       — default batch size applied to all topics
        CONSUMER_BATCH_TIMEOUT_MS — default batch timeout applied to all topics
    """
    bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"].split(",")
    gcs_bucket = os.environ["GCS_BUCKET_NAME"]
    schema_registry_url = os.environ["KAFKA_SCHEMA_REGISTRY_URL"]
    schema_registry_auth = os.getenv("KAFKA_SCHEMA_REGISTRY_AUTH")

    raw_topic_configs = json.loads(os.environ["KAFKA_TOPIC_CONFIG_JSON"])
    topic_configs = [TopicConfig(**tc) for tc in raw_topic_configs]

    default_batch_size = int(os.getenv("CONSUMER_BATCH_SIZE", "5000"))
    default_batch_timeout = int(os.getenv("CONSUMER_BATCH_TIMEOUT_MS", "5000"))
    for tc in topic_configs:
        tc.batch_size = tc.batch_size or default_batch_size
        tc.batch_timeout_ms = tc.batch_timeout_ms or default_batch_timeout

    config = ConsumerConfig(
        bootstrap_servers=[s.strip() for s in bootstrap_servers],
        gcs_bucket=gcs_bucket,
        schema_registry=SchemaRegistryConfig(
            url=schema_registry_url,
            basic_auth_user_info=schema_registry_auth,
        ),
        topics=topic_configs,
    )
    return DataPlatformConsumer(config)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(threadName)s — %(message)s",
    )

    consumer = build_consumer_from_env()
    consumer.install_signal_handlers()
    consumer.start()

    # Block main thread until consumer stops (signal-driven shutdown)
    while consumer.is_running():
        time.sleep(5)
