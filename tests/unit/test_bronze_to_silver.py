"""Tests for src/processing/bronze_to_silver.py (local Spark + Delta)."""

import pytest

from src.processing.bronze_to_silver import (
    add_row_hash,
    apply_scd_type2,
    dedup_latest,
    merge_scd_type1,
    parse_job_specs,
    validate_and_quarantine,
)

spark = pytest.importorskip("pyspark.sql")


# ---------------------------------------------------------------------------
# Spec parsing
# ---------------------------------------------------------------------------


class TestSpecParsing:
    def test_minimal_spec(self):
        specs = parse_job_specs(
            [{"name": "payments", "source_path": "/b", "target_path": "/s", "primary_keys": ["id"]}]
        )
        assert specs[0].scd_type == 1
        assert specs[0].order_column == "_kafka_timestamp_ms"

    def test_missing_keys_raise(self):
        with pytest.raises(ValueError, match="primary_keys"):
            parse_job_specs([{"name": "x", "source_path": "/b", "target_path": "/s"}])

    def test_bad_scd_type_raises(self):
        with pytest.raises(ValueError, match="scd_type"):
            parse_job_specs(
                [
                    {
                        "name": "x",
                        "source_path": "/b",
                        "target_path": "/s",
                        "primary_keys": ["id"],
                        "scd_type": 3,
                    }
                ]
            )


# ---------------------------------------------------------------------------
# Dedup + validation
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("spark")
class TestDedupLatest:
    def test_keeps_highest_order_per_key(self, spark):
        rows = [
            {"id": "a", "amount": 1.0, "_kafka_timestamp_ms": 100},
            {"id": "a", "amount": 2.0, "_kafka_timestamp_ms": 300},  # wins
            {"id": "a", "amount": 3.0, "_kafka_timestamp_ms": 200},
            {"id": "b", "amount": 9.0, "_kafka_timestamp_ms": 50},
        ]
        df = spark.createDataFrame(rows)
        out = dedup_latest(df, ["id"], "_kafka_timestamp_ms").collect()

        assert len(out) == 2
        by_id = {r["id"]: r["amount"] for r in out}
        assert by_id == {"a": 2.0, "b": 9.0}


class TestValidateQuarantine:
    def test_null_pk_and_missing_required_split(self, spark):
        from src.processing.bronze_to_silver import SilverJobSpec

        spec = SilverJobSpec(
            name="t",
            source_path="/b",
            target_path="/s",
            primary_keys=["id"],
            required_columns=["amount"],
        )
        rows = [
            {"id": "ok-1", "amount": 5.0},
            {"id": None, "amount": 5.0},
            {"id": "ok-2", "amount": None},
        ]
        df = spark.createDataFrame(rows)
        clean, bad = validate_and_quarantine(df, spec)

        assert clean.count() == 1
        assert bad is not None
        reasons = {r["quarantine_reason"] for r in bad.collect()}
        assert reasons == {"null_primary_key", "missing_required"}


# ---------------------------------------------------------------------------
# Row hash
# ---------------------------------------------------------------------------


class TestRowHash:
    def test_excludes_meta_and_is_stable(self, spark):
        r1 = spark.createDataFrame([{"id": "x", "v": 1, "_kafka_offset": 9}])
        r2 = spark.createDataFrame([{"id": "x", "v": 1, "_kafka_offset": 77}])
        h1 = add_row_hash(r1).collect()[0]["_row_hash"]
        h2 = add_row_hash(r2).collect()[0]["_row_hash"]
        assert h1 == h2  # meta column excluded

        r3 = spark.createDataFrame([{"id": "x", "v": 2, "_kafka_offset": 9}])
        h3 = add_row_hash(r3).collect()[0]["_row_hash"]
        assert h3 != h1

    def test_null_coalesced_not_breaking_hash(self, spark):
        from pyspark.sql.types import StringType, StructField, StructType

        schema = StructType(
            [
                StructField("id", StringType()),
                StructField("v", StringType()),
            ]
        )
        r1 = spark.createDataFrame([{"id": "x", "v": None}], schema)
        h = add_row_hash(r1).collect()[0]["_row_hash"]
        assert isinstance(h, int)


# ---------------------------------------------------------------------------
# SCD Type 1 merge
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("tmp_delta_table")
class TestMergeScdType1:
    def test_upsert_updates_existing_and_inserts_new(self, spark, tmp_delta_table, tmp_path):
        initial = tmp_delta_table(
            [
                {"id": "a", "status": "created"},
                {"id": "b", "status": "created"},
            ]
        )
        updates = spark.createDataFrame(
            [
                {"id": "a", "status": "shipped"},  # update
                {"id": "c", "status": "created"},  # insert
            ]
        )

        merge_scd_type1(initial, updates, ["id"])

        out = {r["id"]: r["status"] for r in spark.read.format("delta").load(initial).collect()}
        assert out == {"a": "shipped", "b": "created", "c": "created"}

    def test_creates_target_when_absent(self, spark, tmp_path):
        target = str(tmp_path / "new_table")
        updates = spark.createDataFrame([{"id": "a", "v": 1}])
        merge_scd_type1(target, updates, ["id"])
        assert spark.read.format("delta").load(target).count() == 1


# ---------------------------------------------------------------------------
# SCD Type 2 merge
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("tmp_delta_table")
class TestMergeScdType2:
    def test_change_creates_history_noop_does_not(self, spark, tmp_path):
        target = str(tmp_path / "scd2")
        v1 = spark.createDataFrame([{"id": "a", "addr": "9 Oak"}])
        apply_scd_type2(spark, target, v1, ["id"])

        # identical redelivery — must NOT create a new version
        apply_scd_type2(
            spark, target, spark.createDataFrame([{"id": "a", "addr": "9 Oak"}]), ["id"]
        )
        rows = spark.read.format("delta").load(target).collect()
        assert len(rows) == 1
        assert rows[0]["_is_current"] is True

        # real change — old row retired, new current inserted
        apply_scd_type2(
            spark, target, spark.createDataFrame([{"id": "a", "addr": "18 Main"}]), ["id"]
        )
        rows = sorted(
            spark.read.format("delta").load(target).collect(), key=lambda r: r["_valid_from"]
        )

        assert len(rows) == 2
        assert [r["_is_current"] for r in rows] == [False, True]
        assert [r["addr"] for r in rows] == ["9 Oak", "18 Main"]
        assert rows[0]["_valid_to"] is not None
        assert rows[1]["_valid_to"] is None

    def test_multiple_keys_independent(self, spark, tmp_path):
        target = str(tmp_path / "scd2multi")
        apply_scd_type2(
            spark, target, spark.createDataFrame([{"id": "a", "v": 1}, {"id": "b", "v": 1}]), ["id"]
        )
        apply_scd_type2(
            spark, target, spark.createDataFrame([{"id": "a", "v": 2}, {"id": "b", "v": 1}]), ["id"]
        )

        rows = spark.read.format("delta").load(target).collect()
        currents = {r["id"]: r["v"] for r in rows if r["_is_current"]}
        assert currents == {"a": 2, "b": 1}
        assert len(rows) == 3  # a-v1 retired, a-v2 current, b-v1 current
