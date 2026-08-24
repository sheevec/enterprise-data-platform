"""Tests for src/maintenance/table_optimization.py (local Spark + Delta)."""

import pytest

from src.maintenance.table_optimization import (
    MaintenanceConfig,
    TableSpec,
    build_optimize_sql,
    parse_table_specs,
    run_maintenance,
)

spark = pytest.importorskip("pyspark.sql")


class TestSpecParsing:
    def test_parse_full(self):
        specs = parse_table_specs(
            [{"path": "gs://b/t", "z_order_by": ["customer_id"], "partition_filter": "d >= 1"}]
        )
        assert specs[0].z_order_by == ["customer_id"]

    def test_missing_path_raises(self):
        with pytest.raises(ValueError, match="path"):
            parse_table_specs([{"z_order_by": ["x"]}])


class TestOptimizeSql:
    def test_compact_only(self):
        sql = build_optimize_sql(TableSpec(path="/tmp/t"))
        assert sql == "OPTIMIZE delta.`/tmp/t`"

    def test_zorder_and_predicate(self):
        sql = build_optimize_sql(
            TableSpec(path="/tmp/t", z_order_by=["a", "b"], partition_filter="d >= 3")
        )
        assert "ZORDER BY (a, b)" in sql
        assert "WHERE d >= 3" in sql
        # predicate must precede ZORDER clause
        assert sql.index("WHERE") < sql.index("ZORDER")


@pytest.mark.usefixtures("spark")
class TestRunMaintenance:
    def test_end_to_end_on_local_delta(self, spark, tmp_path):
        table_path = str(tmp_path / "events")
        rows = [
            {"customer_id": f"c{i % 7}", "amount": float(i), "event_date": "2025-01-01"}
            for i in range(50)
        ]
        spark.createDataFrame(rows).repartition(10).write.format("delta").save(table_path)

        config = MaintenanceConfig(
            tables=[TableSpec(path=table_path, z_order_by=["customer_id"])],
            vacuum_retention_hours=168,
            vacuum_dry_run=True,
        )
        results = run_maintenance(spark, config)

        assert len(results) == 1
        entry = results[0]
        assert "optimize_error" not in entry
        assert entry["optimize"]["files_removed"] == 10  # repartition(10) input files
        assert entry["optimize"]["files_added"] >= 1
        assert "vacuum_error" not in entry
        assert entry["vacuum"]["dry_run"] is True

        # data survived optimize intact
        assert spark.read.format("delta").load(table_path).count() == 50
