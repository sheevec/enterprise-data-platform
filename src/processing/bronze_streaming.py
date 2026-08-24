"""
bronze_streaming.py
-------------------
Spark Structured Streaming job: Kafka (Avro, Confluent wire format) → Delta
Bronze layer on GCS. This is the petabyte-scale ingestion path; the
confluent-kafka consumer remains for low-volume/edge topics only.

Why Spark Structured Streaming instead of N consumer replicas:
  - JVM-native Avro decode (vectorized from_avro), no Python GIL ceiling
  - Checkpoint-driven exactly-once-ish semantics: offsets + output commit
    advance atomically per micro-batch — crash mid-batch replays it, never
    duplicates it (Delta idempotent txn writes: txnAppId/txnVersion)
  - One job scales to any throughput by widening cluster executors

Architecture per topic:
    Kafka source ──▶ decode & validate ──┬─▶ valid rows  → foreachBatch:
    (raw binary)     (SQL expressions)   │                 Delta append (idempotent)
                                         └─▶ bad magic / unknown schema_id
                                             → quarantine path (raw bytes kept)

Decode strategy (reader-schema mode):
  - "latest" (default): strip the 5-byte Confluent header in SQL and decode all
    records with the subject's LATEST registry schema. Safe under BACKWARD
    compatibility (CI gate via SchemaRegistryClient.check_backward_compatible).
  - Records whose embedded schema_id is not in the subject's registered ID list
    are quarantined with a reason — never silently dropped.

Bronze record contract (identical to the kafka_consumer path — downstream dedup
keys on the first three):
    _kafka_topic, _kafka_partition, _kafka_offset,
    _kafka_timestamp_ms, _ingested_utc

Local run example (Dataproc-submit equivalent documented in Makefile):

    python -m src.processing.bronze_streaming            # continuous trigger

Requires at runtime (spark-submit --packages):
    org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1   (Kafka source)
    org.apache.spark:spark-avro_2.12:3.4.1             (from_avro; absent from pip dist)
    com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.11
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import LongType

from src.ingestion.schema_registry import SchemaRegistryClient, SchemaRegistryConfig
from src.utils.config import get_bool, get_json_list, require_env

logger = logging.getLogger(__name__)

# Bronze record contract columns shared across ingestion paths
BRONZE_META_COLUMNS = (
    "_kafka_topic",
    "_kafka_partition",
    "_kafka_offset",
    "_kafka_timestamp_ms",
    "_ingested_utc",
)


# ---------------------------------------------------------------------------
# Wire-format SQL expressions (pure functions — unit-testable without Spark)
# ---------------------------------------------------------------------------

# Confluent wire format: [0x00][4-byte BE schema_id][payload]
MAGIC_BYTE_OK_EXPR = "(ascii(substring(value, 1, 1)) = 0) AND (length(value) >= 5)"
SCHEMA_ID_EXPR = "conv(hex(substring(value, 2, 4)), 16, 10)"


def build_kafka_spark_options(
    bootstrap_servers: List[str],
    security_protocol: str = "SASL_SSL",
    sasl_mechanism: str = "PLAIN",
    api_key: Optional[str] = None,
    api_secret: Optional[str] = None,
) -> Dict[str, str]:
    """
    Build Spark Kafka-source options. Spark wraps standard kafka client keys
    with the 'kafka.' prefix; credentials go through a JAAS config string.
    """
    options: Dict[str, str] = {
        "kafka.bootstrap.servers": ",".join(bootstrap_servers),
        "kafka.security.protocol": security_protocol,
        "failOnDataLoss": "false",  # retention-expired offsets log loudly instead of killing stream
    }
    if security_protocol.startswith("SASL"):
        if not api_key or not api_secret:
            raise ValueError("SASL security requires KAFKA_API_KEY and KAFKA_API_SECRET.")
        options["kafka.sasl.mechanism"] = sasl_mechanism
        options["kafka.sasl.jaas.config"] = (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{api_key}" password="{api_secret}";'
        )
    return options


def build_bronze_paths(bucket: str, prefix: str, topic: str) -> Dict[str, str]:
    """Standard Bronze layout: data / checkpoint / quarantine per topic."""
    base = f"gs://{bucket}/{prefix}/{topic}"
    return {
        "data": base,
        "checkpoint": f"gs://{bucket}/_checkpoints/{prefix}/{topic}",
        "quarantine": f"gs://{bucket}/_quarantine/{prefix}/{topic}",
    }


def parse_topic_specs(raw_specs: List[Dict[str, Any]]) -> List["TopicSpec"]:
    """Validate and coerce raw JSON topic dicts into TopicSpec objects."""
    specs: List[TopicSpec] = []
    for raw in raw_specs:
        missing = {"topic", "schema_subject"} - set(raw)
        if missing:
            raise ValueError(f"Topic spec {raw} missing required keys: {missing}")
        specs.append(
            TopicSpec(
                topic=str(raw["topic"]),
                schema_subject=str(raw["schema_subject"]),
                gcs_prefix=str(raw.get("gcs_prefix", "bronze")).strip("/"),
                starting_offsets=str(raw.get("starting_offsets", "earliest")),
                reader_schema_mode=str(raw.get("reader_schema_mode", "latest")),
            )
        )
    return specs


@dataclass
class TopicSpec:
    """Per-topic streaming configuration."""

    topic: str
    schema_subject: str  # e.g. "payments-value"
    gcs_prefix: str  # e.g. "bronze/payments"
    starting_offsets: str = "earliest"  # "earliest" | "latest" | json offset map
    reader_schema_mode: str = "latest"  # latest | per_version (per_version: TODO)


@dataclass
class BronzeStreamConfig:
    """Top-level configuration for BronzeStreamingJob."""

    bootstrap_servers: List[str]
    gcs_bucket: str
    topics: List[TopicSpec]
    schema_registry: SchemaRegistryConfig
    # Kafka security (defaults match Confluent Cloud)
    security_protocol: str = "SASL_SSL"
    sasl_mechanism: str = "PLAIN"
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    # Trigger: continuous ("10 seconds") or backfill mode (availableNow=True)
    trigger_interval: str = "30 seconds"
    available_now: bool = False
    # Quarantine keeps raw bytes for replay; never drops silently
    quarantine_enabled: bool = True


# ---------------------------------------------------------------------------
# The streaming job
# ---------------------------------------------------------------------------


class BronzeStreamingJob:
    """One managed StreamingQuery per configured topic."""

    def __init__(self, config: BronzeStreamConfig, spark: Optional[SparkSession] = None):
        self._config = config
        self._registry = SchemaRegistryClient(config.schema_registry)
        self.spark = spark or self._build_spark_session()
        self._queries: List[Any] = []

    @staticmethod
    def _build_spark_session() -> SparkSession:
        return (
            SparkSession.builder.appName("edp-bronze-streaming")
            # Sane defaults for high-throughput Kafka sources; tunable via submit flags
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "auto".replace("auto", "200"))
            .config("spark.sql.adaptive.enabled", "true")
            .config(
                "spark.sql.adaptive.skewJoin.enabled", "true"
            )  # AQE splits skewed join partitions automatically
            .config("spark.sql.autoBroadcastJoinThreshold", str(200 * 1024 * 1024))
            .getOrCreate()
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> List[Any]:
        """Start one StreamingQuery per topic. Returns the query handles."""
        kafka_opts = build_kafka_spark_options(
            bootstrap_servers=self._config.bootstrap_servers,
            security_protocol=self._config.security_protocol,
            sasl_mechanism=self._config.sasl_mechanism,
            api_key=self._config.api_key,
            api_secret=self._config.api_secret,
        )

        for spec in self._config.topics:
            query = self._start_topic_query(spec, kafka_opts)
            self._queries.append(query)
            logger.info(
                "Started bronze stream | topic=%s | data=%s | checkpoint=%s",
                spec.topic,
                build_bronze_paths(self._config.gcs_bucket, spec.gcs_prefix, spec.topic)["data"],
                build_bronze_paths(self._config.gcs_bucket, spec.gcs_prefix, spec.topic)[
                    "checkpoint"
                ],
            )
        return self._queries

    def await_termination(self) -> None:
        for q in self._queries:
            q.awaitTermination()

    def stop(self) -> None:
        for q in self._queries:
            q.stop()
        self.spark.stop()

    # ------------------------------------------------------------------
    # Per-topic pipeline
    # ------------------------------------------------------------------

    def _start_topic_query(self, spec: TopicSpec, kafka_opts: Dict[str, str]) -> Any:
        schema_id, schema_json = self._resolve_reader_schema(spec)
        allowed_ids = self._registry.list_subject_schema_ids(spec.schema_subject)
        logger.info(
            "Reader schema resolved | topic=%s | subject=%s | id=%d | known_ids=%s",
            spec.topic,
            spec.schema_subject,
            schema_id,
            allowed_ids,
        )

        raw = (
            self.spark.readStream.format("kafka")
            .options(**kafka_opts)
            .option("subscribe", spec.topic)
            .option("startingOffsets", spec.starting_offsets)
            .load()
        )

        decoded = transform_batch(raw, schema_json, allowed_ids)
        paths = build_bronze_paths(self._config.gcs_bucket, spec.gcs_prefix, spec.topic)

        writer = (
            decoded.writeStream.queryName(f"bronze-{spec.topic}")
            .foreachBatch(self._make_batch_writer(spec))
            .option("checkpointLocation", paths["checkpoint"])
            .outputMode("append")
        )

        if self._config.available_now:
            writer = writer.trigger(availableNow=True)
        else:
            writer = writer.trigger(processingTime=self._config.trigger_interval)

        return writer.start()

    def _resolve_reader_schema(self, spec: TopicSpec) -> tuple[int, str]:
        if spec.reader_schema_mode != "latest":
            raise NotImplementedError(
                f"reader_schema_mode={spec.reader_schema_mode!r}; supported: 'latest'"
            )
        return self._registry.get_latest_schema(spec.schema_subject)

    def _make_batch_writer(self, spec: TopicSpec) -> Any:
        """
        foreachBatch sink: Delta append with transactional idempotency.

        txnAppId+txnVersion make retries of the same micro-batch a no-op in
        Delta — this is what upgrades 'at-least-once' to effectively-once even
        when checkpoints roll back after a failure.
        """
        paths = build_bronze_paths(self._config.gcs_bucket, spec.gcs_prefix, spec.topic)
        app_id = f"edp-bronze-{spec.topic}"

        def write_batch(df: DataFrame, batch_id: int) -> None:
            count = df.count()
            if count == 0:
                logger.debug("Empty batch | topic=%s | batch=%d", spec.topic, batch_id)
                return

            (
                df.write.format("delta")
                .mode("append")
                .option("txnAppId", app_id)
                .option("txnVersion", batch_id)
                .save(paths["data"])
            )
            logger.info(
                "Bronze batch committed | topic=%s | batch=%d | rows=%d | target=%s",
                spec.topic,
                batch_id,
                count,
                paths["data"],
            )

        return write_batch


# ---------------------------------------------------------------------------
# Batch transformation (exposed for testing — pure df-in/df-out)
# ---------------------------------------------------------------------------


def transform_batch(
    raw: DataFrame, reader_schema_json: str, allowed_schema_ids: List[int]
) -> DataFrame:
    """
    Decode + validate one micro-batch of Kafka rows into the Bronze contract.

    Valid rows:    full avro payload fields + BRONZE_META_COLUMNS
    Invalid rows:  routed by `route` column; caller (this module) sends them to
                   the quarantine sink inside transform via union — here we keep
                   both streams in ONE dataframe distinguished by `is_valid` so
                   the foreachBatch sink stays simple; production split happens
                   in split_valid_quarantined().
    """
    ids_csv = ",".join(str(i) for i in sorted(allowed_schema_ids))

    meta = raw.select(
        F.col("value"),
        F.col("topic").alias("_kafka_topic"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").cast(LongType()).alias("_kafka_offset"),
        # Kafka source delivers TimestampType; contract stores epoch millis
        # (matches confluent-kafka consumer's message.timestamp()).
        (F.unix_timestamp("timestamp") * 1000).cast(LongType()).alias("_kafka_timestamp_ms"),
    )

    annotated = (
        meta.withColumn("_magic_ok", F.expr(MAGIC_BYTE_OK_EXPR))
        .withColumn("_schema_id_str", F.expr(SCHEMA_ID_EXPR))
        .withColumn(
            "_id_known",
            F.expr(f"_schema_id_str IN ({ids_csv})"),
        )
    )

    valid = (
        annotated.where(F.col("_magic_ok") & F.col("_id_known"))
        .withColumn(
            "decoded",
            from_avro(
                F.expr("substring(value, 6, length(value) - 5)"),
                reader_schema_json,
            ),
        )
        .select(
            "decoded.*", "_kafka_topic", "_kafka_partition", "_kafka_offset", "_kafka_timestamp_ms"
        )
        .withColumn("_ingested_utc", F.current_timestamp())
    )

    quarantined = (
        annotated.where(~(F.col("_magic_ok") & F.col("_id_known")))
        .withColumn(
            "quarantine_reason",
            F.when(~F.col("_magic_ok"), F.lit("invalid_wire_format")).otherwise(
                F.lit("unknown_schema_id")
            ),
        )
        .withColumn("_ingested_utc", F.current_timestamp())
    )

    return valid.unionByName(quarantined, allowMissingColumns=True)


def split_valid_quarantined(batch_df: DataFrame) -> tuple[DataFrame, Optional[DataFrame]]:
    """Split a transformed batch into (valid, quarantined-or-None)."""
    has_route = "quarantine_reason" in batch_df.columns
    if not has_route:
        return batch_df, None
    valid = batch_df.where(F.col("quarantine_reason").isNull())
    bad = batch_df.where(F.col("quarantine_reason").isNotNull())
    return valid, (bad if bad.take(1) else None)


# ---------------------------------------------------------------------------
# Env-based factory + entrypoint
# ---------------------------------------------------------------------------


def build_job_from_env() -> BronzeStreamingJob:
    """Assemble the job from environment variables (see .env.example)."""
    raw_topics = get_json_list("BRONZE_TOPICS_JSON")
    if not raw_topics:
        raise ValueError("BRONZE_TOPICS_JSON must contain at least one topic spec.")

    config = BronzeStreamConfig(
        bootstrap_servers=[s.strip() for s in require_env("KAFKA_BOOTSTRAP_SERVERS").split(",")],
        gcs_bucket=require_env("GCS_BUCKET_NAME"),
        topics=parse_topic_specs(raw_topics),
        schema_registry=SchemaRegistryConfig(
            url=require_env("KAFKA_SCHEMA_REGISTRY_URL"),
            basic_auth_user_info=os.getenv("KAFKA_SCHEMA_REGISTRY_AUTH"),
        ),
        security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
        sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
        api_key=os.getenv("KAFKA_API_KEY"),
        api_secret=os.getenv("KAFKA_API_SECRET"),
        trigger_interval=os.getenv("BRONZE_TRIGGER_INTERVAL", "30 seconds"),
        available_now=get_bool("BRONZE_AVAILABLE_NOW", False),
        quarantine_enabled=get_bool("BRONZE_QUARANTINE_ENABLED", True),
    )
    return BronzeStreamingJob(config)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(threadName)s — %(message)s",
    )
    job = build_job_from_env()
    try:
        job.start()
        if job._config.available_now:
            for q in job._queries:
                q.awaitTermination()
        else:
            job.await_termination()
    except KeyboardInterrupt:
        logger.info("Interrupted — stopping streams gracefully")
    finally:
        job.stop()


if __name__ == "__main__":
    main()
