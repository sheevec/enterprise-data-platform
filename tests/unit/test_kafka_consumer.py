"""Unit tests for src/ingestion/kafka_consumer.py (no Kafka/GCS/network required)."""

import io
import json

import fastavro
import pytest

from src.ingestion.kafka_consumer import (
    ConsumerMetrics,
    SchemaRegistryClient,
    SchemaRegistryConfig,
    TopicConfig,
    build_security_conf,
    merge_offset,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload

    def raise_for_status(self):
        return None


class _FakeSession:
    def __init__(self, payload):
        self._payload = payload
        self.calls = []

    def get(self, url, timeout=None):
        self.calls.append(url)
        return _FakeResponse(self._payload)


def _make_avro_message(schema: dict, record: dict, schema_id: int = 42) -> bytes:
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, fastavro.parse_schema(schema), record)
    payload = buf.getvalue()
    magic = b"\x00" + schema_id.to_bytes(4, byteorder="big")
    return magic + payload


SCHEMA = {
    "type": "record",
    "name": "PaymentEvent",
    "fields": [
        {"name": "payment_id", "type": "string"},
        {"name": "amount_cents", "type": "int"},
    ],
}


# ---------------------------------------------------------------------------
# Schema Registry wire-format decoding
# ---------------------------------------------------------------------------


class TestSchemaRegistryClient:
    def test_decode_message_roundtrip(self):
        registry = SchemaRegistryClient(SchemaRegistryConfig(url="http://fake"))
        registry._session = _FakeSession({"schema": json.dumps(SCHEMA)})

        raw = _make_avro_message(SCHEMA, {"payment_id": "p-1", "amount_cents": 2500})
        decoded = registry.decode_message(raw)

        assert decoded == {"payment_id": "p-1", "amount_cents": 2500}
        # Schema fetched once, then served from cache
        assert len(registry._session.calls) == 1
        registry.decode_message(
            _make_avro_message(SCHEMA, {"payment_id": "p-2", "amount_cents": 1})
        )
        assert len(registry._session.calls) == 1

    def test_invalid_magic_byte_raises(self):
        registry = SchemaRegistryClient(SchemaRegistryConfig(url="http://fake"))
        registry._session = _FakeSession({"schema": "{}"})

        bad_payload = b"\xff" + b"\x00\x00\x00\x01" + b"junk"
        with pytest.raises(ValueError, match="wire format"):
            registry.decode_message(bad_payload)

    def test_truncated_message_raises(self):
        registry = SchemaRegistryClient(SchemaRegistryConfig(url="http://fake"))
        registry._session = _FakeSession({"schema": "{}"})
        with pytest.raises(ValueError):
            registry.decode_message(b"\x00\x00")


# ---------------------------------------------------------------------------
# Offset bookkeeping
# ---------------------------------------------------------------------------


class TestMergeOffset:
    def test_tracks_max_next_offset_per_partition(self):
        offsets = {}
        merge_offset(offsets, "t", 0, offset=10)
        merge_offset(offsets, "t", 0, offset=5)  # out-of-order arrival
        merge_offset(offsets, "t", 1, offset=99)
        merge_offset(offsets, "t", 0, offset=12)

        assert offsets[("t", 0)] == 13  # next-offset-to-commit semantics
        assert offsets[("t", 1)] == 100


# ---------------------------------------------------------------------------
# Security configuration
# ---------------------------------------------------------------------------


class TestBuildSecurityConf:
    def test_sasl_ssl_requires_credentials(self, monkeypatch):
        monkeypatch.delenv("KAFKA_SECURITY_PROTOCOL", raising=False)
        monkeypatch.delenv("KAFKA_API_KEY", raising=False)
        monkeypatch.delenv("KAFKA_API_SECRET", raising=False)

        with pytest.raises(ValueError, match="KAFKA_API_KEY"):
            build_security_conf(["broker.example.com:9092"])

    def test_sasl_ssl_with_credentials(self, monkeypatch):
        monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
        monkeypatch.setenv("KAFKA_SASL_MECHANISM", "PLAIN")
        monkeypatch.setenv("KAFKA_API_KEY", "key")
        monkeypatch.setenv("KAFKA_API_SECRET", "secret")

        conf = build_security_conf(["broker.example.com:9092"])
        assert conf["security.protocol"] == "SASL_SSL"
        assert conf["sasl.mechanism"] == "PLAIN"
        assert conf["sasl.username"] == "key"

    def test_plaintext_allowed_for_local(self, monkeypatch):
        monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")

        conf = build_security_conf(["localhost:9092"])
        assert conf["security.protocol"] == "PLAINTEXT"


# ---------------------------------------------------------------------------
# Metrics container
# ---------------------------------------------------------------------------


class TestConsumerMetrics:
    def test_to_dict_shape(self):
        m = ConsumerMetrics(messages_consumed=10, consumer_lag={"t:0": 7})
        snapshot = m.to_dict()

        assert snapshot["messages_consumed"] == 10
        assert snapshot["consumer_lag"] == {"t:0": 7}
        assert "batches_written" in snapshot
        assert "rebalances" in snapshot


# ---------------------------------------------------------------------------
# Config defaults
# ---------------------------------------------------------------------------


class TestTopicConfig:
    def test_defaults(self):
        tc = TopicConfig(
            topic="payments",
            group_id="g",
            gcs_prefix="bronze/payments",
            dlq_topic="payments.dlq",
            schema_subject="payments-value",
        )
        assert tc.batch_size == 5000
        assert tc.batch_timeout_ms == 5000
