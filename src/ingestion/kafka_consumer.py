"""
kafka_consumer.py
-----------------
DataPlatformConsumer: A production-grade Kafka consumer that reads Avro-serialized
messages from multiple topics, validates them against Confluent Schema Registry,
and writes partitioned Parquet files to the GCS Bronze layer.

Features:
  - Multi-topic subscription with per-topic configuration
  - Confluent Schema Registry Avro deserialization
  - GCS Bronze layer writes partitioned by source/date/hour
  - Dead letter queue (DLQ) for failed/unparseable messages
  - Prometheus-compatible metrics tracking (messages consumed, errors, consumer lag)
  - Graceful shutdown handling
"""

from __future__ import annotations

import io
import json
import logging
import os
import signal
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

import fastavro
from google.cloud import storage
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.structs import OffsetAndMetadata

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration dataclasses
# ---------------------------------------------------------------------------


@dataclass
class SchemaRegistryConfig:
    """Connection settings for Confluent Schema Registry."""

    url: str
    basic_auth_user_info: Optional[str] = None  # "key:secret" for Confluent Cloud


@dataclass
class TopicConfig:
    """Per-topic consumption settings."""

    topic: str
    group_id: str
    gcs_prefix: str          # e.g. "bronze/payments"
    dlq_topic: str           # Dead-letter queue topic name
    schema_subject: str      # Schema Registry subject name, e.g. "payments-value"
    batch_size: int = 500
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
    enable_auto_commit: bool = False
    session_timeout_ms: int = 30_000
    heartbeat_interval_ms: int = 10_000
    max_poll_records: int = 500
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
    # Per-partition lag snapshot {topic_partition_str: lag}
    consumer_lag: Dict[str, int] = field(default_factory=dict)
    last_metrics_snapshot_utc: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "messages_consumed": self.messages_consumed,
            "messages_written_to_gcs": self.messages_written_to_gcs,
            "messages_sent_to_dlq": self.messages_sent_to_dlq,
            "deserialization_errors": self.deserialization_errors,
            "gcs_write_errors": self.gcs_write_errors,
            "dlq_send_errors": self.dlq_send_errors,
            "batches_written": self.batches_written,
            "consumer_lag": self.consumer_lag,
            "last_metrics_snapshot_utc": self.last_metrics_snapshot_utc,
        }

    def log_snapshot(self) -> None:
        self.last_metrics_snapshot_utc = datetime.now(timezone.utc).isoformat()
        logger.info("Consumer metrics snapshot: %s", json.dumps(self.to_dict(), indent=2))


# ---------------------------------------------------------------------------
# Schema Registry client (lightweight, no confluent_kafka dependency)
# ---------------------------------------------------------------------------


class SchemaRegistryClient:
    """
    Minimal Schema Registry client that fetches and caches Avro schemas
    identified by schema ID embedded in Confluent wire-format messages.

    Wire format: [0x00][4-byte schema_id][avro_payload]
    """

    MAGIC_BYTE = 0x00
    SCHEMA_ID_LENGTH = 4

    def __init__(self, config: SchemaRegistryConfig) -> None:
        import requests

        self._base_url = config.url.rstrip("/")
        self._session = requests.Session()
        if config.basic_auth_user_info:
            key, secret = config.basic_auth_user_info.split(":", 1)
            self._session.auth = (key, secret)
        self._schema_cache: Dict[int, Any] = {}  # schema_id -> parsed avro schema

    def get_schema(self, schema_id: int) -> Any:
        """Return a parsed fastavro schema for the given schema ID (cached)."""
        if schema_id in self._schema_cache:
            return self._schema_cache[schema_id]

        url = f"{self._base_url}/schemas/ids/{schema_id}"
        response = self._session.get(url, timeout=10)
        response.raise_for_status()
        schema_str = response.json()["schema"]
        parsed = fastavro.parse_schema(json.loads(schema_str))
        self._schema_cache[schema_id] = parsed
        logger.debug("Fetched and cached schema id=%d from registry", schema_id)
        return parsed

    def decode_message(self, raw_bytes: bytes) -> Dict[str, Any]:
        """
        Decode a Confluent wire-format Avro message.
        Returns the deserialized Python dict.
        """
        if len(raw_bytes) < 5 or raw_bytes[0] != self.MAGIC_BYTE:
            raise ValueError(
                f"Invalid Confluent wire format: magic byte missing or wrong "
                f"(got 0x{raw_bytes[0]:02x} expected 0x00)"
            )

        schema_id = int.from_bytes(raw_bytes[1:5], byteorder="big")
        schema = self.get_schema(schema_id)
        avro_payload = raw_bytes[5:]

        with io.BytesIO(avro_payload) as buf:
            record = fastavro.schemaless_reader(buf, schema)
        return record


# ---------------------------------------------------------------------------
# GCS writer with partitioned paths
# ---------------------------------------------------------------------------


class GCSBronzeWriter:
    """
    Writes batches of Avro-decoded records as newline-delimited JSON (NDJSON)
    or Parquet to GCS, partitioned by year/month/day/hour.

    Path pattern:
        gs://{bucket}/{prefix}/year={yyyy}/month={mm}/day={dd}/hour={hh}/{uuid}.parquet
    """

    def __init__(self, bucket_name: str, timeout_seconds: int = 60) -> None:
        self._client = storage.Client()
        self._bucket = self._client.bucket(bucket_name)
        self._timeout = timeout_seconds

    def _partition_path(self, prefix: str, ts: datetime) -> str:
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

        # Convert records to a DataFrame and serialize to Parquet in memory
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
# Main consumer class
# ---------------------------------------------------------------------------


class DataPlatformConsumer:
    """
    Production Kafka consumer for the Enterprise Data Platform Bronze layer.

    Lifecycle:
        consumer = DataPlatformConsumer(config)
        consumer.start()          # begins consuming in background threads
        ...
        consumer.stop()           # graceful shutdown
        metrics = consumer.get_metrics()

    Each TopicConfig results in an independent consumer thread consuming that
    topic's messages, deserializing Avro payloads via Schema Registry, batching
    records, writing Parquet files to GCS, and routing failures to a DLQ topic.
    """

    def __init__(self, config: ConsumerConfig) -> None:
        self._config = config
        self._schema_registry = SchemaRegistryClient(config.schema_registry)
        self._gcs_writer = GCSBronzeWriter(
            config.gcs_bucket,
            timeout_seconds=config.gcs_write_timeout_seconds,
        )
        self._metrics = ConsumerMetrics()
        self._metrics_lock = threading.Lock()
        self._running = False
        self._threads: List[threading.Thread] = []
        self._shutdown_event = threading.Event()

        # Register OS signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

        logger.info(
            "DataPlatformConsumer initialized | bucket=%s | topics=%s",
            config.gcs_bucket,
            [t.topic for t in config.topics],
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
            t = threading.Thread(
                target=self._consume_topic,
                args=(topic_cfg,),
                name=f"consumer-{topic_cfg.topic}",
                daemon=True,
            )
            t.start()
            self._threads.append(t)
            logger.info("Started consumer thread for topic=%s", topic_cfg.topic)

        # Start a background metrics logging thread
        metrics_thread = threading.Thread(
            target=self._metrics_reporter,
            name="metrics-reporter",
            daemon=True,
        )
        metrics_thread.start()
        self._threads.append(metrics_thread)

    def stop(self, timeout_seconds: int = 30) -> None:
        """Signal all consumer threads to stop and wait for them to finish."""
        logger.info("Initiating graceful shutdown...")
        self._running = False
        self._shutdown_event.set()

        for t in self._threads:
            t.join(timeout=timeout_seconds)
            if t.is_alive():
                logger.warning("Thread %s did not stop within %ds", t.name, timeout_seconds)

        self._metrics.log_snapshot()
        logger.info("DataPlatformConsumer stopped.")

    def get_metrics(self) -> Dict[str, Any]:
        """Return a snapshot of current consumer metrics."""
        with self._metrics_lock:
            return self._metrics.to_dict()

    def is_running(self) -> bool:
        return self._running

    # ------------------------------------------------------------------
    # Internal consumption logic
    # ------------------------------------------------------------------

    def _consume_topic(self, topic_cfg: TopicConfig) -> None:
        """
        Main loop for a single topic consumer thread.
        Polls Kafka, deserializes Avro messages, batches them, writes to GCS,
        and commits offsets only after a successful GCS write.
        """
        consumer = self._build_kafka_consumer(topic_cfg)
        dlq_producer = self._build_dlq_producer()

        logger.info(
            "Topic consumer ready | topic=%s | group=%s",
            topic_cfg.topic,
            topic_cfg.group_id,
        )

        batch: List[Dict[str, Any]] = []
        batch_offsets: Dict[TopicPartition, OffsetAndMetadata] = {}
        batch_start_time = time.monotonic()

        try:
            while self._running and not self._shutdown_event.is_set():
                records = consumer.poll(timeout_ms=topic_cfg.batch_timeout_ms)

                for partition_records in records.values():
                    for message in partition_records:
                        self._update_lag(consumer, topic_cfg.topic)

                        try:
                            decoded = self._deserialize_message(message.value)
                            decoded["_kafka_offset"] = message.offset
                            decoded["_kafka_partition"] = message.partition
                            decoded["_kafka_timestamp_ms"] = message.timestamp
                            decoded["_ingested_utc"] = datetime.now(timezone.utc).isoformat()
                            batch.append(decoded)

                            tp = TopicPartition(message.topic, message.partition)
                            batch_offsets[tp] = OffsetAndMetadata(message.offset + 1, None)

                            with self._metrics_lock:
                                self._metrics.messages_consumed += 1

                        except Exception as deser_exc:
                            logger.warning(
                                "Deserialization failed | topic=%s | partition=%d | offset=%d | error=%s",
                                message.topic,
                                message.partition,
                                message.offset,
                                deser_exc,
                            )
                            with self._metrics_lock:
                                self._metrics.deserialization_errors += 1
                            self._send_to_dlq(dlq_producer, topic_cfg.dlq_topic, message, deser_exc)

                # Flush batch when size or time threshold is reached
                elapsed_ms = (time.monotonic() - batch_start_time) * 1000
                should_flush = (
                    len(batch) >= topic_cfg.batch_size
                    or (elapsed_ms >= topic_cfg.batch_timeout_ms and len(batch) > 0)
                )

                if should_flush:
                    self._flush_batch(
                        consumer=consumer,
                        batch=batch,
                        offsets=batch_offsets,
                        topic_cfg=topic_cfg,
                    )
                    batch = []
                    batch_offsets = {}
                    batch_start_time = time.monotonic()

            # Flush any remaining records on shutdown
            if batch:
                logger.info(
                    "Flushing %d remaining records on shutdown for topic=%s",
                    len(batch),
                    topic_cfg.topic,
                )
                self._flush_batch(
                    consumer=consumer,
                    batch=batch,
                    offsets=batch_offsets,
                    topic_cfg=topic_cfg,
                )

        except Exception as exc:
            logger.error(
                "Unhandled exception in consumer thread for topic=%s: %s",
                topic_cfg.topic,
                exc,
                exc_info=True,
            )
        finally:
            consumer.close()
            dlq_producer.flush()
            dlq_producer.close()
            logger.info("Consumer thread exiting for topic=%s", topic_cfg.topic)

    def _flush_batch(
        self,
        consumer: KafkaConsumer,
        batch: List[Dict[str, Any]],
        offsets: Dict[TopicPartition, OffsetAndMetadata],
        topic_cfg: TopicConfig,
    ) -> None:
        """Write a batch of records to GCS, then commit Kafka offsets."""
        if not batch:
            return

        partition_ts = datetime.now(timezone.utc)
        file_id = f"{partition_ts.strftime('%Y%m%dT%H%M%S%f')}-{threading.get_ident()}"

        try:
            self._gcs_writer.write_batch(
                records=batch,
                prefix=topic_cfg.gcs_prefix,
                partition_ts=partition_ts,
                file_id=file_id,
            )
            # Only commit offsets after a confirmed successful GCS write
            consumer.commit(offsets=offsets)

            with self._metrics_lock:
                self._metrics.messages_written_to_gcs += len(batch)
                self._metrics.batches_written += 1

            logger.debug(
                "Flushed batch | topic=%s | records=%d | file_id=%s",
                topic_cfg.topic,
                len(batch),
                file_id,
            )

        except Exception as gcs_exc:
            logger.error(
                "GCS write failed | topic=%s | records=%d | error=%s",
                topic_cfg.topic,
                len(batch),
                gcs_exc,
                exc_info=True,
            )
            with self._metrics_lock:
                self._metrics.gcs_write_errors += 1
            # Do NOT commit offsets — records will be re-consumed on restart

    def _deserialize_message(self, raw_bytes: bytes) -> Dict[str, Any]:
        """Decode a Confluent wire-format Avro message using Schema Registry."""
        return self._schema_registry.decode_message(raw_bytes)

    def _send_to_dlq(
        self,
        producer: KafkaProducer,
        dlq_topic: str,
        original_message: Any,
        error: Exception,
    ) -> None:
        """
        Publish a failed message to the Dead Letter Queue topic.
        The DLQ record wraps the original raw bytes along with error metadata.
        """
        dlq_record = {
            "original_topic": original_message.topic,
            "original_partition": original_message.partition,
            "original_offset": original_message.offset,
            "original_timestamp_ms": original_message.timestamp,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "failed_at_utc": datetime.now(timezone.utc).isoformat(),
            "raw_value_hex": original_message.value.hex() if original_message.value else None,
        }

        try:
            producer.send(
                dlq_topic,
                value=json.dumps(dlq_record).encode("utf-8"),
                key=(original_message.key or b""),
            )
            with self._metrics_lock:
                self._metrics.messages_sent_to_dlq += 1
            logger.warning(
                "Sent message to DLQ | dlq_topic=%s | original_offset=%d | error=%s",
                dlq_topic,
                original_message.offset,
                error,
            )
        except Exception as dlq_exc:
            with self._metrics_lock:
                self._metrics.dlq_send_errors += 1
            logger.error(
                "Failed to send message to DLQ | dlq_topic=%s | error=%s",
                dlq_topic,
                dlq_exc,
                exc_info=True,
            )

    def _update_lag(self, consumer: KafkaConsumer, topic: str) -> None:
        """Compute and record consumer lag for each assigned partition."""
        try:
            assigned = consumer.assignment()
            end_offsets = consumer.end_offsets(list(assigned))
            committed = {
                tp: consumer.committed(tp) or 0 for tp in assigned if tp.topic == topic
            }
            lag_map: Dict[str, int] = {}
            for tp, end in end_offsets.items():
                if tp.topic != topic:
                    continue
                current = committed.get(tp, 0)
                lag = max(0, end - current)
                lag_map[f"{tp.topic}:{tp.partition}"] = lag

            with self._metrics_lock:
                self._metrics.consumer_lag.update(lag_map)

        except Exception as lag_exc:
            logger.debug("Could not compute consumer lag: %s", lag_exc)

    # ------------------------------------------------------------------
    # Kafka client factories
    # ------------------------------------------------------------------

    def _build_kafka_consumer(self, topic_cfg: TopicConfig) -> KafkaConsumer:
        cfg = self._config
        consumer = KafkaConsumer(
            topic_cfg.topic,
            bootstrap_servers=cfg.bootstrap_servers,
            group_id=topic_cfg.group_id,
            auto_offset_reset=cfg.auto_offset_reset,
            enable_auto_commit=cfg.enable_auto_commit,
            session_timeout_ms=cfg.session_timeout_ms,
            heartbeat_interval_ms=cfg.heartbeat_interval_ms,
            max_poll_records=cfg.max_poll_records,
            fetch_max_wait_ms=cfg.fetch_max_wait_ms,
            # Receive raw bytes; deserialization is handled by SchemaRegistryClient
            key_deserializer=None,
            value_deserializer=None,
            # Security (TLS + SASL/PLAIN for Confluent Cloud)
            security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
            sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", None),
            sasl_plain_username=os.getenv("KAFKA_API_KEY", None),
            sasl_plain_password=os.getenv("KAFKA_API_SECRET", None),
        )
        return consumer

    def _build_dlq_producer(self) -> KafkaProducer:
        cfg = self._config
        return KafkaProducer(
            bootstrap_servers=cfg.bootstrap_servers,
            retries=cfg.dlq_retries,
            acks="all",
            value_serializer=None,  # Already bytes
            security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
            sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", None),
            sasl_plain_username=os.getenv("KAFKA_API_KEY", None),
            sasl_plain_password=os.getenv("KAFKA_API_SECRET", None),
        )

    # ------------------------------------------------------------------
    # Metrics reporter
    # ------------------------------------------------------------------

    def _metrics_reporter(self) -> None:
        """Periodically log a metrics snapshot every 60 seconds."""
        while self._running and not self._shutdown_event.wait(timeout=60):
            with self._metrics_lock:
                self._metrics.log_snapshot()

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
        KAFKA_BOOTSTRAP_SERVERS  — comma-separated list, e.g. "broker1:9092,broker2:9092"
        KAFKA_SCHEMA_REGISTRY_URL — e.g. "https://schema-registry.example.com"
        KAFKA_SCHEMA_REGISTRY_AUTH — "key:secret" for Confluent Cloud (optional)
        GCS_BUCKET_NAME           — target GCS bucket for Bronze layer
        KAFKA_TOPIC_CONFIG_JSON   — JSON list of TopicConfig dicts (see TopicConfig dataclass)
    """
    bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"].split(",")
    gcs_bucket = os.environ["GCS_BUCKET_NAME"]
    schema_registry_url = os.environ["KAFKA_SCHEMA_REGISTRY_URL"]
    schema_registry_auth = os.getenv("KAFKA_SCHEMA_REGISTRY_AUTH")

    raw_topic_configs = json.loads(os.environ["KAFKA_TOPIC_CONFIG_JSON"])
    topic_configs = [TopicConfig(**tc) for tc in raw_topic_configs]

    config = ConsumerConfig(
        bootstrap_servers=bootstrap_servers,
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
    consumer.start()

    # Block main thread until consumer stops (signal-driven shutdown)
    while consumer.is_running():
        time.sleep(5)
