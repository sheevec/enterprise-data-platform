"""Tests for src/governance/gdpr_erasure.py (local Spark + Delta)."""

import json

import pytest

from src.governance.gdpr_erasure import ErasureRequest, ErasureTarget, GdprEraser

spark = pytest.importorskip("pyspark.sql")


@pytest.fixture()
def lakehouse(spark, tmp_path):
    """Bronze + Silver + Gold Delta tables sharing customer_id 'victim-1'."""
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType(
        [
            StructField("customer_id", StringType()),
            StructField("payload", StringType()),
        ]
    )
    rows = [
        ("customer-1", "ok"),
        ("victim-1", "erase-me"),
        ("customer-3", "ok"),
        ("victim-1", "erase-me-too"),
        ("customer-5", "ok"),
    ]

    paths = {}
    for layer in ("bronze", "silver", "gold"):
        path = str(tmp_path / layer)
        spark.createDataFrame(rows, schema).write.format("delta").save(path)
        paths[layer] = path
    return paths


@pytest.mark.usefixtures("spark")
class TestGdprErasure:
    def test_erasure_propagates_across_all_layers(self, spark, lakehouse, tmp_path):
        request = ErasureRequest(
            subjects=["victim-1"],
            targets=[
                ErasureTarget(lakehouse["bronze"], "customer_id", layer="bronze"),
                ErasureTarget(lakehouse["silver"], "customer_id", layer="silver"),
                ErasureTarget(lakehouse["gold"], "customer_id", layer="gold"),
            ],
            requested_by="dpo@example.com",
        )
        report = GdprEraser(spark, audit_log_dir=str(tmp_path / "audit")).erase(request)

        assert report.all_succeeded
        assert report.total_rows_deleted == 6  # 2 rows x 3 layers
        assert all(r.rows_deleted == 2 for r in report.results)

        for path in lakehouse.values():
            remaining = {r["customer_id"] for r in spark.read.format("delta").load(path).collect()}
            assert remaining == {"customer-1", "customer-3", "customer-5"}

    def test_audit_log_written_with_provenance(self, spark, lakehouse, tmp_path):
        audit_dir = tmp_path / "audit"
        request = ErasureRequest(
            subjects=["victim-1"],
            targets=[ErasureTarget(lakehouse["silver"], "customer_id")],
            requested_by="dpo@example.com",
        )
        GdprEraser(spark, audit_log_dir=str(audit_dir)).erase(request)

        files = list(audit_dir.glob("*.jsonl"))
        assert len(files) == 1
        record = json.loads(files[0].read_text())
        assert record["requested_by"] == "dpo@example.com"
        assert record["subjects_count"] == 1
        assert record["total_rows_deleted"] == 2
        assert record["results"][0]["success"] is True

    def test_missing_table_recorded_as_failure_does_not_abort_batch(
        self, spark, lakehouse, tmp_path
    ):
        request = ErasureRequest(
            subjects=["customer-1"],
            targets=[
                ErasureTarget(str(tmp_path / "does-not-exist"), "customer_id"),
                ErasureTarget(lakehouse["gold"], "customer_id"),
            ],
            requested_by="ops",
        )
        report = GdprEraser(spark, audit_log_dir=str(tmp_path / "audit")).erase(request)

        assert not report.all_succeeded
        failed = [r for r in report.results if not r.success]
        assert len(failed) == 1
        assert "does-not-exist" in failed[0].table_path
        # healthy table still processed
        gold = next(r for r in report.results if r.table_path == lakehouse["gold"])
        assert gold.success and gold.rows_deleted == 1

    def test_empty_subjects_rejected(self, spark, lakehouse, tmp_path):
        with pytest.raises(ValueError, match="no subjects"):
            GdprEraser(spark, audit_log_dir=str(tmp_path / "a")).erase(
                ErasureRequest(subjects=[], targets=[], requested_by="x")
            )
