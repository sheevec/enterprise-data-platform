"""
table_optimization.py
---------------------
Nightly Delta table maintenance: compaction, Z-ordering, and vacuum.

Why this exists (the small-files problem):
  Streaming ingestion commits one file per micro-batch per partition. At
  30s-1m trigger intervals that is thousands of tiny Parquet files per day —
  each adds metadata overhead, explodes listing costs, and forces readers to
  open millions of file handles. Compaction rewrites them into ~1GB files.

Z-ORDER clusters co-accessed columns' data together inside those files, so a
query filtering on `customer_id, event_date` reads 5 files instead of 500 —
directly cutting BigQuery/Dataproc scan spend.

VACUUM removes stale files left behind by MERGE/OPTIMIZE rewrites. Retention
(default 7 days) protects concurrent readers and time-travel; never set it to
0 in production.

Run nightly via Airflow/Dataproc:
    spark-submit src/maintenance/table_optimization.py
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List

from pyspark.sql import SparkSession

from src.utils.config import get_bool, get_int, get_json_list

logger = logging.getLogger(__name__)


@dataclass
class TableSpec:
    """Maintenance definition for one Delta table."""

    path: str
    z_order_by: List[str] = field(default_factory=list)  # empty = compact only
    # Bound optimize cost on huge tables: only touch recent partitions.
    # SQL predicate against partition columns, e.g. "event_date >= current_date() - 3"
    partition_filter: str = ""


@dataclass
class MaintenanceConfig:
    tables: List[TableSpec]
    vacuum_retention_hours: int = 168  # 7 days — time-travel safety floor
    vacuum_dry_run: bool = True  # report deletable files; delete needs explicit opt-in


def parse_table_specs(raw_tables: List[Dict[str, Any]]) -> List[TableSpec]:
    specs: List[TableSpec] = []
    for raw in raw_tables:
        if "path" not in raw:
            raise ValueError(f"Table spec {raw} missing 'path'")
        specs.append(
            TableSpec(
                path=str(raw["path"]),
                z_order_by=[str(c) for c in raw.get("z_order_by", [])],
                partition_filter=str(raw.get("partition_filter", "")),
            )
        )
    return specs


# ---------------------------------------------------------------------------
# Operations (thin wrappers over Delta SQL — kept as functions for testing)
# ---------------------------------------------------------------------------


def build_optimize_sql(spec: TableSpec) -> str:
    """Compose OPTIMIZE ... ZORDER BY ... WHERE ... for one table."""
    sql = f"OPTIMIZE delta.`{spec.path}`"
    if spec.partition_filter:
        sql += f" WHERE {spec.partition_filter}"
    if spec.z_order_by:
        cols = ", ".join(spec.z_order_by)
        sql += f" ZORDER BY ({cols})"
    return sql


def run_maintenance(spark: SparkSession, config: MaintenanceConfig) -> List[Dict[str, Any]]:
    """Execute optimize (+vacuum) for every table. Returns per-table results."""
    results: List[Dict[str, Any]] = []

    for spec in config.tables:
        entry: Dict[str, Any] = {"path": spec.path}

        try:
            optimize_sql = build_optimize_sql(spec)
            row = spark.sql(optimize_sql).collect()[0]
            # Delta 2.x returns Row(path, metrics=Row(...)); 3.x may flatten.
            raw_metrics = row["metrics"] if "metrics" in row.asDict() else row
            metrics = raw_metrics.asDict() if hasattr(raw_metrics, "asDict") else dict(raw_metrics)
            entry["optimize"] = {
                "sql": optimize_sql,
                "files_added": metrics.get("numFilesAdded"),
                "files_removed": metrics.get("numFilesRemoved"),
            }
            logger.info(
                "OPTIMIZE done | path=%s | removed=%s | added=%s",
                spec.path,
                metrics.get("numFilesRemoved"),
                metrics.get("numFilesAdded"),
            )
        except Exception as exc:
            entry["optimize_error"] = str(exc)
            logger.error("OPTIMIZE failed | path=%s | error=%s", spec.path, exc)
            results.append(entry)
            continue

        try:
            vacuum_sql = f"VACUUM delta.`{spec.path}` RETAIN {config.vacuum_retention_hours} HOURS"
            if config.vacuum_dry_run:
                vacuum_sql += " DRY RUN"
                deletable = [row["path"] for row in spark.sql(vacuum_sql).collect()]
                entry["vacuum"] = {"dry_run": True, "deletable_files": len(deletable)}
                logger.info(
                    "VACUUM dry-run | path=%s | deletable_files=%d",
                    spec.path,
                    len(deletable),
                )
            else:
                spark.sql(vacuum_sql)
                entry["vacuum"] = {"dry_run": False}
                logger.info(
                    "VACUUM executed | path=%s | retention_hours=%d",
                    spec.path,
                    config.vacuum_retention_hours,
                )
        except Exception as exc:
            entry["vacuum_error"] = str(exc)
            logger.error("VACUUM failed | path=%s | error=%s", spec.path, exc)

        results.append(entry)

    return results


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def build_config_from_env() -> MaintenanceConfig:
    raw_tables = get_json_list("OPTIMIZE_TABLES_JSON")
    if not raw_tables:
        raise ValueError("OPTIMIZE_TABLES_JSON must contain at least one table spec.")
    return MaintenanceConfig(
        tables=parse_table_specs(raw_tables),
        vacuum_retention_hours=get_int("VACUUM_RETENTION_HOURS", 168),
        vacuum_dry_run=get_bool("VACUUM_DRY_RUN", True),
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s"
    )
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("edp-table-optimization")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .getOrCreate()
    )
    try:
        results = run_maintenance(spark, build_config_from_env())
        failures = [r for r in results if "optimize_error" in r or "vacuum_error" in r]
        logger.info("Maintenance complete | tables=%d | failures=%d", len(results), len(failures))
        if failures:
            raise SystemExit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
