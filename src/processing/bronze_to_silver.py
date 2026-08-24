"""
bronze_to_silver.py
-------------------
Bronze → Silver processing: cleanse, deduplicate, and MERGE change data into
conformed Delta tables. Runs as a Structured Streaming job (continuous or
availableNow backfill); the merge executes per micro-batch inside foreachBatch.

Pipeline per configured job:

    Bronze Delta ─▶ validate/quarantine ─▶ dedup-latest (window) ─▶ SCD merge
                   (null PKs, bad rows)   row_number() by order_col    ▲
                                                                     │
                                              Type 1: overwrite current state
                                              Type 2: history (valid_from/to,
                                                      _is_current, _row_hash)

Key decisions baked in:
  - DEDUP BEFORE MERGE: one micro-batch can contain multiple versions of the
    same key (Kafka replays, backfills) — merging all of them is both wasted
    work and nondeterministic. Keep only the latest version per key.
  - QUARANTINE, NEVER SILENT DROP: invalid rows land in a quarantine Delta
    path with a machine-readable reason; counts are logged per batch.
  - SCD2 change detection via xxhash64 over business columns — cheap,
    deterministic, and lets the second MERGE skip identical re-deliveries.
  - Streaming semantics: checkpoint + foreachBatch give effectively-once
    against Delta; late data beyond the watermark lands in the NEXT batch's
    dedup window (watermark bounds state, not correctness — merges are keyed).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.observability.lineage_tracker import LineageDataset, build_emitter_from_env, new_run_id
from src.utils.config import get_bool, get_json_list

logger = logging.getLogger(__name__)

SCD_TYPE_1 = 1
SCD_TYPE_2 = 2

# Columns excluded from business-state hashing / SCD comparisons
_META_PREFIX = "_"
_SCD_SYSTEM_COLUMNS = ("_valid_from", "_valid_to", "_is_current", "_row_hash")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass
class SilverJobSpec:
    """One Bronze→Silver pipeline definition."""

    name: str
    source_path: str  # Bronze Delta root for this entity
    target_path: str  # Silver Delta table path
    primary_keys: List[str]
    # Column deciding which record "wins" on duplicates (event-time preferred)
    order_column: str = "_kafka_timestamp_ms"
    scd_type: int = SCD_TYPE_1  # 1 = overwrite, 2 = full history
    required_columns: List[str] = field(default_factory=list)
    quarantine_path: Optional[str] = None  # defaults to <target>/../_quarantine/<name>
    starting_offsets: Optional[str] = None  # Delta streaming start (e.g. version json)


@dataclass
class SilverConfig:
    jobs: List[SilverJobSpec]
    trigger_interval: str = "1 minute"
    available_now: bool = False


def parse_job_specs(raw_jobs: List[Dict[str, Any]]) -> List[SilverJobSpec]:
    specs: List[SilverJobSpec] = []
    for raw in raw_jobs:
        missing = {"name", "source_path", "target_path", "primary_keys"} - set(raw)
        if missing:
            raise ValueError(f"Silver job spec {raw} missing keys: {missing}")
        if raw.get("scd_type") not in (None, SCD_TYPE_1, SCD_TYPE_2):
            raise ValueError(
                f"Unsupported scd_type={raw.get('scd_type')!r} in spec {raw['name']!r}"
            )
        specs.append(
            SilverJobSpec(
                name=str(raw["name"]),
                source_path=str(raw["source_path"]),
                target_path=str(raw["target_path"]),
                primary_keys=[str(k) for k in raw["primary_keys"]],
                order_column=str(raw.get("order_column", "_kafka_timestamp_ms")),
                scd_type=int(raw.get("scd_type", SCD_TYPE_1)),
                required_columns=[str(c) for c in raw.get("required_columns", [])],
            )
        )
    return specs


# ---------------------------------------------------------------------------
# Transform steps (pure df-in/df-out — unit-testable)
# ---------------------------------------------------------------------------


def validate_and_quarantine(
    batch: DataFrame, spec: SilverJobSpec
) -> tuple[DataFrame, Optional[DataFrame]]:
    """
    Split batch into (clean, quarantined-or-None).

    Rules:
      - NULL primary keys are unmergeable → quarantine("null_primary_key")
      - Missing required columns' values (nulls) → quarantine("missing_required")
    """
    pk_violation = F.lit(False)
    for key in spec.primary_keys:
        pk_violation = pk_violation | F.col(key).isNull()

    req_violation = F.lit(False)
    for col_name in spec.required_columns:
        req_violation = req_violation | F.col(col_name).isNull()

    clean = batch.where(~(pk_violation | req_violation))
    bad = batch.where(pk_violation | req_violation).withColumn(
        "quarantine_reason",
        F.when(pk_violation, F.lit("null_primary_key")).otherwise(F.lit("missing_required")),
    )
    bad_out = None if bad.take(1) == [] else bad
    return clean, bad_out


def write_quarantine(bad: DataFrame, path: str, batch_id: int) -> int:
    """Persist quarantined rows with batch lineage; returns count."""
    count = bad.count()
    if count == 0:
        return 0
    (
        bad.write.format("delta")
        .mode("append")
        .option("txnAppId", f"silver-quarantine-{path}")
        .option("txnVersion", batch_id)
        .save(path)
    )
    logger.warning("Quarantined %d rows | target=%s | batch=%d", count, path, batch_id)
    return count


def dedup_latest(df: DataFrame, primary_keys: List[str], order_column: str) -> DataFrame:
    """Keep only the highest-`order_column` row per business key."""
    window = Window.partitionBy(*primary_keys).orderBy(F.col(order_column).desc())
    return df.withColumn("_rn", F.row_number().over(window)).where(F.col("_rn") == 1).drop("_rn")


def add_row_hash(df: DataFrame, exclude: tuple[str, ...] = ()) -> DataFrame:
    """xxhash64 over all non-meta business columns (deterministic order)."""
    cols = sorted(
        c
        for c in df.columns
        if not c.startswith(_META_PREFIX) and c not in _SCD_SYSTEM_COLUMNS and c not in exclude
    )
    return df.withColumn(
        "_row_hash", F.xxhash64(*[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols])
    )


def merge_scd_type1(target_path: str, updates: DataFrame, primary_keys: List[str]) -> None:
    """Upsert current state. No history."""
    join_condition = " AND ".join(f"t.{k} = s.{k}" for k in primary_keys)
    if not _table_exists(target_path):
        updates.write.format("delta").mode("overwrite").save(target_path)
        return
    (
        DeltaTable.forPath(updates.sparkSession, target_path)
        .alias("t")
        .merge(updates.alias("s"), join_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def merge_scd_type2(
    spark: SparkSession, target_path: str, updates: DataFrame, primary_keys: List[str]
) -> None:
    """Public alias for apply_scd_type2."""
    apply_scd_type2(spark, target_path, updates, primary_keys)


def ensure_scd2_table(spark: SparkSession, target_path: str, schema_df: DataFrame) -> None:
    """Create the SCD2 target skeleton if absent (empty but typed)."""
    if _table_exists(target_path):
        return
    (
        schema_df.limit(0)
        .withColumn("_valid_from", F.current_timestamp())
        .withColumn("_valid_to", F.lit(None).cast("timestamp"))
        .withColumn("_is_current", F.lit(True))
        .withColumn("_row_hash", F.lit(""))
        .write.format("delta")
        .mode("overwrite")
        .save(target_path)
    )


def apply_scd_type2(
    spark: SparkSession, target_path: str, updates: DataFrame, primary_keys: List[str]
) -> None:
    """
    Full-history merge in two statements:

      1. Retire currents whose hash changed:  t._is_current=true AND hash differs
         → set _is_current=false, _valid_to=now.
      2. Insert every update that does NOT already have an identical current row.
         Covers brand-new keys AND new versions of changed keys; skips no-op
         re-deliveries of unchanged state (the whole point of _row_hash).
    """
    hashed = add_row_hash(updates)
    ensure_scd2_table(spark, target_path, hashed)
    keys_join = " AND ".join(f"t.{k} <=> s.{k}" for k in primary_keys)

    target = DeltaTable.forPath(spark, target_path).alias("t")

    # 1. retire changed currents (condition prevents retiring identical re-deliveries)
    (
        target.merge(
            hashed.alias("s"),
            f"{keys_join} AND t._is_current = true",
        )
        .whenMatchedUpdate(
            condition="t._row_hash <> s._row_hash",
            set={
                "_is_current": "false",
                "_valid_to": "current_timestamp()",
            },
        )
        .execute()
    )

    # refresh handle after first merge
    target = DeltaTable.forPath(spark, target_path).alias("t")

    # 2. insert new/changed versions lacking an identical current row
    # NOTE: _row_hash IS passed through from source — only validity/system
    # defaults are substituted. A NULL stored hash would silently break the
    # change-detection predicates above.
    insert_values: Dict[str, Any] = {
        c: f"s.{c}" for c in hashed.columns if c not in ("_valid_from", "_valid_to", "_is_current")
    }
    insert_values.update(
        {
            "_valid_from": "current_timestamp()",
            "_valid_to": "cast(null as timestamp)",
            "_is_current": "true",
        }
    )
    (
        target.merge(
            hashed.alias("s"),
            f"{keys_join} AND t._is_current = true AND t._row_hash = s._row_hash",
        )
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )


def _table_exists(path: str) -> bool:
    spark = SparkSession.getActiveSession()
    assert spark is not None
    try:
        DeltaTable.forPath(spark, path)
        return True
    except Exception as exc:  # AnalysisException/Py4J variants differ across versions
        logger.debug("Table probe failed for %s: %s", path, exc)
        return False


# ---------------------------------------------------------------------------
# Streaming job
# ---------------------------------------------------------------------------


class BronzeToSilverJob:
    """One managed query per SilverJobSpec; merges micro-batches into Delta."""

    def __init__(self, config: SilverConfig, spark: Optional[SparkSession] = None):
        self._config = config
        self.spark = spark or self._build_session()
        self._queries: List[Any] = []

    @staticmethod
    def _build_session() -> SparkSession:
        return (
            SparkSession.builder.appName("edp-bronze-to-silver")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.autoBroadcastJoinThreshold", str(200 * 1024 * 1024))
            .getOrCreate()
        )

    def start(self) -> List[Any]:
        for spec in self._config.jobs:
            reader = self.spark.readStream.format("delta").option("skipChangeCommits", "true")
            if spec.starting_offsets:
                reader = reader.option("startingVersion", spec.starting_offsets)
            stream = reader.load(spec.source_path)

            writer = (
                stream.writeStream.queryName(f"silver-{spec.name}")
                .foreachBatch(self._make_batch_handler(spec))
                .option(
                    "checkpointLocation",
                    f"{spec.target_path.rstrip('/')}/../../_checkpoints/silver/{spec.name}",
                )
                .outputMode("update")
            )
            if self._config.available_now:
                writer = writer.trigger(availableNow=True)
            else:
                writer = writer.trigger(processingTime=self._config.trigger_interval)

            q = writer.start()
            self._queries.append(q)
            logger.info("Silver stream started | job=%s | target=%s", spec.name, spec.target_path)
        return self._queries

    def await_termination(self) -> None:
        for q in self._queries:
            q.awaitTermination()

    def stop(self) -> None:
        for q in self._queries:
            q.stop()
        self.spark.stop()

    def _make_batch_handler(self, spec: SilverJobSpec) -> Any:
        quarantine_path = spec.quarantine_path or f"{spec.source_path.rstrip('/')}_quarantine"
        emitter = build_emitter_from_env()

        def handle(batch: DataFrame, batch_id: int) -> None:
            run_id = new_run_id()
            inputs = [LineageDataset(name=spec.source_path, namespace="edp-gcs")]
            outputs = [LineageDataset(name=spec.target_path, namespace="edp-gcs")]
            emitter.emit_start(f"silver.{spec.name}", run_id, inputs, outputs)
            try:
                self._process_batch(spec, batch, batch_id, quarantine_path)
                emitter.emit_complete(f"silver.{spec.name}", run_id, inputs, outputs)
            except Exception as exc:
                emitter.emit_fail(
                    f"silver.{spec.name}",
                    run_id,
                    inputs,
                    outputs,
                    error_message=f"{type(exc).__name__}: {exc}",
                )
                raise

        return handle

    def _process_batch(
        self, spec: SilverJobSpec, batch: DataFrame, batch_id: int, quarantine_path: str
    ) -> None:
        if not batch.rdd.isEmpty():  # cheap existence check
            clean, bad = validate_and_quarantine(batch, spec)
            quarantined = write_quarantine(bad, quarantine_path, batch_id) if bad is not None else 0

            deduped = dedup_latest(clean, spec.primary_keys, spec.order_column)

            if spec.scd_type == SCD_TYPE_2:
                apply_scd_type2(self.spark, spec.target_path, deduped, spec.primary_keys)
            else:
                merge_scd_type1(spec.target_path, deduped, spec.primary_keys)

            logger.info(
                "Silver batch merged | job=%s | batch=%d | rows=%d | quarantined=%d | scd=t%d",
                spec.name,
                batch_id,
                deduped.count(),
                quarantined,
                spec.scd_type,
            )


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def build_job_from_env() -> BronzeToSilverJob:
    raw_jobs = get_json_list("SILVER_JOBS_JSON")
    if not raw_jobs:
        raise ValueError("SILVER_JOBS_JSON must contain at least one job spec.")
    config = SilverConfig(
        jobs=parse_job_specs(raw_jobs),
        trigger_interval=os.getenv("SILVER_TRIGGER_INTERVAL", "1 minute"),
        available_now=get_bool("SILVER_AVAILABLE_NOW", False),
    )
    return BronzeToSilverJob(config)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(threadName)s — %(message)s",
    )
    job = build_job_from_env()
    try:
        job.start()
        job.await_termination()
    except KeyboardInterrupt:
        logger.info("Interrupted — stopping silver streams gracefully")
    finally:
        job.stop()


if __name__ == "__main__":
    main()
