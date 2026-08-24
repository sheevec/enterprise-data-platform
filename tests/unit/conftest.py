"""Shared fixtures for Spark-backed unit tests (local master, no cluster)."""

import os
import sys

import pytest

spark_session = pytest.importorskip("pyspark.sql")


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    # Workers must use THIS interpreter: bare `python3` from PATH may be a
    # newer Python where pyspark 3.4's typing.io import crashes the worker.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    session = (
        SparkSession.builder.master("local[2]")
        .appName("edp-unit-tests")
        # pip pyspark/delta-spark ship Python wrappers only; JVM jars resolve via Maven
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-avro_2.12:3.4.1,io.delta:delta-core_2.12:2.4.0",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()


@pytest.fixture()
def tmp_delta_table(spark, tmp_path):
    """Factory: write a pandas-ish list of dicts as a Delta table, return its path."""

    def _create(rows: list[dict], path: str | None = None) -> str:
        table_path = path or str(tmp_path / f"table-{abs(hash(tuple(rows[0].keys())))}")
        df = spark.createDataFrame(rows)  # type: ignore[attr-defined]
        df.write.format("delta").mode("overwrite").save(table_path)
        return table_path

    return _create
