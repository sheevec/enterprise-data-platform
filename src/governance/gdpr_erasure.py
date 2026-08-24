"""
gdpr_erasure.py
---------------
GDPR Article 17 (right to erasure) propagation across the lakehouse.

The hard truth about deletion in a lakehouse: Bronze is append-only by
design, and Delta keeps time-travel history. This module does the honest
thing at each layer:

  - Silver/Gold (Delta tables): DELETE by subject key predicate — copy-on-write
    rewrites only affected files; current snapshots are clean immediately.
  - Audit trail: every run writes an immutable JSONL record (who, what, how
    many rows per table). The audit log itself is NEVER erased — regulators
    accept "we deleted the subject and can prove it" over silent silence.
  - Time travel / old snapshots: physical purge completes when VACUUM runs
    past retention. Document this in DSAR responses; do not pretend instant.

Crypto-shredding alternative for truly immutable stores: tokenize PII with
per-subject keys and destroy the key — data remains but is cryptographically
unrecoverable. Wire tokenize strategy (src/processing/pii_masking.py) with
key-per-customer if legal requires erasure from immutable media.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ErasureTarget:
    """One Delta table + the column that carries the subject identifier."""

    table_path: str
    key_column: str  # e.g. customer_id
    layer: str = "silver"  # bronze | silver | gold (label only)


@dataclass
class ErasureRequest:
    subjects: List[str]  # identifier values to erase
    targets: List[ErasureTarget]
    requested_by: str  # DPO / system initiating
    reason: str = "gdpr_art17_dsar"
    request_id: Optional[str] = None


@dataclass
class TableErasureResult:
    table_path: str
    layer: str
    rows_deleted: int
    success: bool
    error: Optional[str] = None


@dataclass
class ErasureReport:
    request_id: str
    requested_by: str
    reason: str
    subjects_count: int
    executed_at_utc: str
    results: List[TableErasureResult] = field(default_factory=list)

    @property
    def total_rows_deleted(self) -> int:
        return sum(r.rows_deleted for r in self.results)

    @property
    def all_succeeded(self) -> bool:
        return all(r.success for r in self.results)

    def to_dict(self) -> Dict[str, Any]:
        """Full serialization incl. computed totals (asdict() drops properties)."""
        data = asdict(self)
        data["total_rows_deleted"] = self.total_rows_deleted
        data["all_succeeded"] = self.all_succeeded
        return data


# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------


class GdprEraser:
    """Executes erasure requests against Delta tables via spark."""

    def __init__(self, spark: Any, audit_log_dir: Optional[str] = None):
        self.spark = spark
        resolved_dir = (
            audit_log_dir or os.getenv("ERASURE_AUDIT_LOG_DIR") or "/tmp/edp_erasure_audit"
        )
        self.audit_log_dir = resolved_dir
        os.makedirs(self.audit_log_dir, exist_ok=True)

    def erase(self, request: ErasureRequest) -> ErasureReport:
        from delta.tables import DeltaTable
        from py4j.protocol import Py4JJavaError

        report = ErasureReport(
            request_id=request.request_id
            or f"dsar-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
            requested_by=request.requested_by,
            reason=request.reason,
            subjects_count=len(request.subjects),
            executed_at_utc=datetime.now(timezone.utc).isoformat(),
        )

        if not request.subjects:
            raise ValueError("Erasure request contains no subjects.")

        # IN-list predicates are parameter-safe here: values are quoted strings
        # composed by us, never raw SQL from callers.
        subjects_csv = ", ".join("'" + s.replace("'", "\\'") + "'" for s in request.subjects)

        for target in request.targets:
            predicate = f"{target.key_column} IN ({subjects_csv})"
            try:
                dt = DeltaTable.forPath(self.spark, target.table_path)
                dt.delete(predicate)
                rows_deleted = int(
                    self._last_operation_metric(target.table_path, "numDeletedRows") or 0
                )
                result = TableErasureResult(
                    table_path=target.table_path,
                    layer=target.layer,
                    rows_deleted=rows_deleted,
                    success=True,
                )
                logger.info(
                    "Erasure applied | layer=%s | path=%s | rows_deleted=%d",
                    target.layer,
                    target.table_path,
                    rows_deleted,
                )
            except Py4JJavaError as exc:
                result = TableErasureResult(
                    table_path=target.table_path,
                    layer=target.layer,
                    rows_deleted=0,
                    success=False,
                    error=str(exc)[:1000],
                )
                logger.error("Erasure FAILED | path=%s | error=%s", target.table_path, exc)
            except Exception as exc:  # missing table etc. — record, continue others
                result = TableErasureResult(
                    table_path=target.table_path,
                    layer=target.layer,
                    rows_deleted=0,
                    success=False,
                    error=f"{type(exc).__name__}: {exc}"[:500],
                )
                logger.error("Erasure FAILED | path=%s | error=%s", target.table_path, exc)

            report.results.append(result)

        self._write_audit_log(report)
        return report

    def _last_operation_metric(self, table_path: str, metric: str) -> Optional[int]:
        try:
            history = self.spark.sql(f"DESCRIBE HISTORY delta.`{table_path}` LIMIT 1").collect()
            if not history:
                return None
            metrics = history[0].asDict().get("operationMetrics") or {}
            value = metrics.get(metric)
            if value in (None, ""):
                return None
            return int(str(value))
        except Exception as exc:
            logger.debug("Could not read operationMetrics for %s: %s", table_path, exc)
            return None

    def _write_audit_log(self, report: ErasureReport) -> None:
        filename = os.path.join(
            self.audit_log_dir, f"{report.executed_at_utc[:10]}_{report.request_id}.jsonl"
        )
        with open(filename, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(report.to_dict(), default=str) + "\n")
        logger.info("Erasure audit written | file=%s", filename)


# ---------------------------------------------------------------------------
# Entrypoint for spark-submit / Airflow task
# ---------------------------------------------------------------------------


def build_request_from_env() -> ErasureRequest:
    raw = json.loads(os.environ["ERASURE_REQUEST_JSON"])
    missing = {"subjects", "targets", "requested_by"} - set(raw)
    if missing:
        raise ValueError(f"ERASURE_REQUEST_JSON missing keys: {missing}")
    return ErasureRequest(
        subjects=[str(s) for s in raw["subjects"]],
        targets=[
            ErasureTarget(
                table_path=t["table_path"],
                key_column=t["key_column"],
                layer=t.get("layer", "silver"),
            )
            for t in raw["targets"]
        ],
        requested_by=str(raw["requested_by"]),
        reason=str(raw.get("reason", "gdpr_art17_dsar")),
    )


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s"
    )
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("edp-gdpr-erasure")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .getOrCreate()
    )
    try:
        report = GdprEraser(spark).erase(build_request_from_env())
        if not report.all_succeeded:
            logger.error(
                "Erasure incomplete | failed_tables=%d",
                sum(1 for r in report.results if not r.success),
            )
            return 1
        logger.info(
            "Erasure complete | tables=%d | total_rows_deleted=%d",
            len(report.results),
            report.total_rows_deleted,
        )
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
