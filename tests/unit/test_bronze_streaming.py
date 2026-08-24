"""Tests for src/processing/bronze_streaming.py.

Pure-logic tests run everywhere. The Spark-backed test of transform_batch runs
when a local SparkSession can start (Java present); it fabricates Confluent
wire-format records in memory — no Kafka, GCS, or network needed.
"""

import io

import fastavro
import pytest

from src.processing.bronze_streaming import (
    BRONZE_META_COLUMNS,
    MAGIC_BYTE_OK_EXPR,
    build_bronze_paths,
    build_kafka_spark_options,
    parse_topic_specs,
)

PAYMENT_SCHEMA = {
    "type": "record",
    "name": "PaymentEvent",
    "fields": [
        {"name": "payment_id", "type": "string"},
        {"name": "amount_cents", "type": "int"},
    ],
}

READER_SCHEMA_JSON = (
    '{"type":"record","name":"PaymentEvent",'
    '"fields":[{"name":"payment_id","type":"string"},{"name":"amount_cents","type":"int"}]}'
)


def _wire_message(schema: dict, record: dict, schema_id: int) -> bytes:
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, fastavro.parse_schema(schema), record)
    return b"\x00" + schema_id.to_bytes(4, "big") + buf.getvalue()


# ---------------------------------------------------------------------------
# Pure logic (no Spark)
# ---------------------------------------------------------------------------


class TestBuildKafkaSparkOptions:
    def test_sasl_ssl_builds_jaas(self):
        opts = build_kafka_spark_options(["b1:9092"], "SASL_SSL", "PLAIN", "key", "secret")
        assert opts["kafka.bootstrap.servers"] == "b1:9092"
        assert opts["kafka.security.protocol"] == "SASL_SSL"
        assert 'username="key"' in opts["kafka.sasl.jaas.config"]
        assert 'password="secret";' in opts["kafka.sasl.jaas.config"]

    def test_sasl_without_creds_raises(self):
        with pytest.raises(ValueError, match="KAFKA_API_KEY"):
            build_kafka_spark_options(["b1:9092"], "SASL_SSL")

    def test_plaintext_has_no_sasl_keys(self):
        opts = build_kafka_spark_options(["localhost:9092"], "PLAINTEXT")
        assert not any("sasl" in k for k in opts)


class TestPaths:
    def test_bronze_layout(self):
        paths = build_bronze_paths("bucket", "bronze/payments", "payments.v2")
        assert paths["data"] == "gs://bucket/bronze/payments/payments.v2"
        assert "_checkpoints" in paths["checkpoint"]
        assert "_quarantine" in paths["quarantine"]


class TestTopicSpecs:
    def test_parse_minimal_spec(self):
        specs = parse_topic_specs([{"topic": "t", "schema_subject": "t-value"}])
        assert specs[0].gcs_prefix == "bronze"
        assert specs[0].starting_offsets == "earliest"
        assert specs[0].reader_schema_mode == "latest"

    def test_missing_keys_raise(self):
        with pytest.raises(ValueError, match="schema_subject"):
            parse_topic_specs([{"topic": "t"}])


class TestWireFormatExpr:
    def test_magic_expr_constant(self):
        # Guard the SQL string against accidental edits — it encodes the
        # Confluent wire format and is verified end-to-end by the Spark test.
        assert "ascii(substring(value, 1, 1)) = 0" in MAGIC_BYTE_OK_EXPR


class TestContract:
    def test_meta_columns_order(self):
        assert BRONZE_META_COLUMNS[:3] == (
            "_kafka_topic",
            "_kafka_partition",
            "_kafka_offset",
        )


# ---------------------------------------------------------------------------
# Spark-backed decode path (shared session fixture from conftest.py;
# fabricates Confluent wire-format records in memory — no Kafka/GCS/network)
# ---------------------------------------------------------------------------

pytest.importorskip("pyspark.sql")


class TestTransformBatch:
    def test_decode_valid_and_quarantine(self, spark):
        from datetime import datetime, timezone

        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            BinaryType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        from src.processing.bronze_streaming import transform_batch

        good = _wire_message(PAYMENT_SCHEMA, {"payment_id": "p-1", "amount_cents": 99}, 7)
        evolved = _wire_message(PAYMENT_SCHEMA, {"payment_id": "p-2", "amount_cents": 1}, 8)
        bad_magic = b"\xffjunkjunk"

        # Kafka source delivers timestamp as TimestampType; contract is epoch-ms
        ts_100 = datetime.fromtimestamp(1_700_000_000, tz=timezone.utc)
        ts_101 = datetime.fromtimestamp(1_700_000_001, tz=timezone.utc)
        ts_102 = datetime.fromtimestamp(1_700_000_002, tz=timezone.utc)

        rows = [
            (good, "payments", 0, 100, ts_100),
            (evolved, "payments", 1, 101, ts_101),
            (bad_magic, "payments", 0, 102, ts_102),
        ]
        schema = StructType(
            [
                StructField("value", BinaryType()),
                StructField("topic", StringType()),
                StructField("partition", LongType()),
                StructField("offset", LongType()),
                StructField("timestamp", TimestampType()),
            ]
        )
        raw = spark.createDataFrame(rows, schema)

        reader_json = READER_SCHEMA_JSON
        out = transform_batch(raw, reader_json, allowed_schema_ids=[7, 8])

        valid = out.where(F.col("quarantine_reason").isNull()).collect()
        quarantined = out.where(F.col("quarantine_reason").isNotNull()).collect()

        assert len(valid) == 2
        ids = {r["payment_id"] for r in valid}
        assert ids == {"p-1", "p-2"}
        offsets = {r["_kafka_offset"] for r in valid}
        assert offsets == {100, 101}
        ms_by_offset = {r["_kafka_offset"]: r["_kafka_timestamp_ms"] for r in valid}
        assert ms_by_offset[100] == 1_700_000_000_000
        assert all(r["_ingested_utc"] is not None for r in valid)

        assert len(quarantined) == 1
        assert quarantined[0]["quarantine_reason"] == "invalid_wire_format"
        assert quarantined[0]["_kafka_offset"] == 102

    def test_unknown_schema_id_quarantined(self, spark):
        from datetime import datetime, timezone

        from pyspark.sql.types import (
            BinaryType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        from src.processing.bronze_streaming import transform_batch

        rogue = _wire_message(PAYMENT_SCHEMA, {"payment_id": "x", "amount_cents": 5}, 999)
        ts = datetime.fromtimestamp(1_700_000_000, tz=timezone.utc)
        raw = spark.createDataFrame(
            [(rogue, "payments", 0, 1, ts)],
            StructType(
                [
                    StructField("value", BinaryType()),
                    StructField("topic", StringType()),
                    StructField("partition", LongType()),
                    StructField("offset", LongType()),
                    StructField("timestamp", TimestampType()),
                ]
            ),
        )
        reader_json = READER_SCHEMA_JSON
        out = transform_batch(raw, reader_json, allowed_schema_ids=[7])

        rows = out.collect()
        assert len(rows) == 1
        assert rows[0]["quarantine_reason"] == "unknown_schema_id"
