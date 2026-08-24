"""
dq_runner.py
------------
spark-submit entrypoint: run the DataQualityFramework against Silver Delta
tables and exit non-zero on a P1 failure — the promotion gate wired into the
Airflow silver DAG between merge and downstream consumption.

Reads a table, samples (bounded by driver memory), validates against the
configured suite for that dataset, and prints the ValidationResult summary.

Env:
    DQ_SUITES_DIR                    — directory of suite JSON files
    DQ_GATE_TABLES_JSON              — [{"table_path": "gs://...", "dataset_name":
                                        "payments_silver", "suite_name": "...",
                                        "layer": "silver", "sample_fraction": 0.1}]
    plus optional DQ_* alerting vars consumed by build_framework_from_env
"""

from __future__ import annotations

import json
import logging
import sys

from pyspark.sql import SparkSession

from src.validation.data_quality import (
    DataLayer,
    DataQualityConfig,
    DataQualityError,
    DataQualityFramework,
    SuiteConfig,
)

logger = logging.getLogger(__name__)


def build_framework() -> DataQualityFramework:
    import os

    return DataQualityFramework(
        DataQualityConfig(
            suites_dir=os.environ["DQ_SUITES_DIR"],
            raise_on_failure=True,
        )
    )


def validate_table(spark: SparkSession, framework: DataQualityFramework, spec: dict) -> str:
    df = spark.read.format("delta").load(spec["table_path"])
    sampled = df.sample(fraction=float(spec.get("sample_fraction", 0.1)), seed=42).toPandas()

    suite_cfg = SuiteConfig(
        suite_name=spec["suite_name"],
        layer=DataLayer(spec.get("layer", "silver")),
        dataset_name=spec["dataset_name"],
    )
    result = framework.validate(sampled, suite_cfg)
    return result.summary


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s"
    )
    import os

    specs = json.loads(os.environ.get("DQ_GATE_TABLES_JSON", "[]"))
    if not specs:
        logger.info("DQ_GATE_TABLES_JSON empty — gate passes trivially")
        return 0

    spark = (
        SparkSession.builder.appName("edp-silver-dq-gate")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .getOrCreate()
    )
    failures = 0
    try:
        framework = build_framework()
        for spec in specs:
            try:
                summary = validate_table(spark, framework, spec)
                logger.info("DQ gate %s | %s", "OK" if "PASS" in summary else "SOFT-FAIL", summary)
            except DataQualityError as dq_exc:
                failures += 1
                logger.error("DQ gate P1 | %s", dq_exc.result.summary)
            except Exception as exc:
                # Infrastructure errors fail the gate too — unknown state ≠ healthy
                failures += 1
                logger.error(
                    "DQ gate infrastructure error | spec=%s | error=%s",
                    spec.get("dataset_name"),
                    exc,
                )
    finally:
        spark.stop()

    if failures:
        logger.error("DQ gate blocked promotion | failing_tables=%d", failures)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
