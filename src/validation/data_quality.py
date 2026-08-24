"""
data_quality.py
---------------
DataQualityFramework: A production wrapper around Great Expectations that enforces
data quality standards across the Enterprise Data Platform's Medallion layers.

Features:
  - Load named expectation suites from a config directory or GCS
  - Validate Pandas/Spark DataFrames against suites with per-column granularity
  - Generate HTML and JSON validation reports
  - Send alerts (Slack, PagerDuty) when quality falls below configurable thresholds
  - Track Data Quality Scores (DQS) over time in a BigQuery monitoring table
  - Support for multiple layers (Bronze, Silver, Gold) with layer-specific thresholds
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pandas as pd
import requests
from google.cloud import bigquery

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Enums and constants
# ---------------------------------------------------------------------------


class DataLayer(str, Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class AlertSeverity(str, Enum):
    P1 = "P1"  # Critical — blocks pipeline promotion
    P2 = "P2"  # High — fires alert, allows promotion with flag
    P3 = "P3"  # Medium — logged only

    @property
    def rank(self) -> int:
        """Numeric rank for severity comparisons (higher = more severe)."""
        return {"P3": 1, "P2": 2, "P1": 3}[self.value]


# Default quality thresholds per layer (fraction of expectations that must pass)
DEFAULT_THRESHOLDS: Dict[DataLayer, float] = {
    DataLayer.BRONZE: 0.90,
    DataLayer.SILVER: 0.95,
    DataLayer.GOLD: 0.99,
}


# ---------------------------------------------------------------------------
# Configuration dataclasses
# ---------------------------------------------------------------------------


@dataclass
class AlertConfig:
    """Alert routing configuration."""

    slack_webhook_url: Optional[str] = None
    pagerduty_routing_key: Optional[str] = None
    # Minimum severity level that triggers a PagerDuty page
    pagerduty_min_severity: AlertSeverity = AlertSeverity.P1
    # Minimum severity level that triggers a Slack message
    slack_min_severity: AlertSeverity = AlertSeverity.P2


@dataclass
class BigQueryMonitoringConfig:
    """Config for writing DQS scores to BigQuery."""

    project: str
    dataset: str
    table: str = "data_quality_scores"

    @property
    def full_table_id(self) -> str:
        return f"{self.project}.{self.dataset}.{self.table}"


@dataclass
class SuiteConfig:
    """Mapping of a named expectation suite to its layer and threshold."""

    suite_name: str
    layer: DataLayer
    dataset_name: str  # Human-readable name, e.g. "payments_raw"
    suite_path: Optional[str] = None  # Local JSON file path (overrides default lookup)
    threshold_override: Optional[float] = None  # Per-suite threshold override

    @property
    def threshold(self) -> float:
        return self.threshold_override or DEFAULT_THRESHOLDS[self.layer]


@dataclass
class DataQualityConfig:
    """Top-level configuration for DataQualityFramework."""

    suites_dir: str  # Directory containing expectation suite JSON files
    alert: AlertConfig = field(default_factory=AlertConfig)
    bq_monitoring: Optional[BigQueryMonitoringConfig] = None
    # If True, raise an exception when quality drops below threshold
    raise_on_failure: bool = True
    # Base path for local report output (always written; durable copy goes to GCS if configured)
    reports_output_dir: str = "/tmp/dq_reports"
    # Optional GCS bucket for durable report storage (e.g. "edp-dq-reports").
    # Local /tmp reports are ephemeral — container restarts lose them.
    reports_gcs_bucket: Optional[str] = None


# ---------------------------------------------------------------------------
# Validation result models
# ---------------------------------------------------------------------------


@dataclass
class ExpectationResult:
    """Result of a single expectation evaluation."""

    expectation_type: str
    column: Optional[str]
    success: bool
    observed_value: Any
    element_count: Optional[int]
    unexpected_count: Optional[int]
    unexpected_percent: Optional[float]
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationResult:
    """Aggregated result of validating a DataFrame against an expectation suite."""

    run_id: str
    dataset_name: str
    suite_name: str
    layer: DataLayer
    evaluated_at_utc: str
    total_expectations: int
    passed_expectations: int
    failed_expectations: int
    data_quality_score: float  # passed / total, range [0, 1]
    threshold: float
    passed_threshold: bool
    severity: AlertSeverity
    expectation_results: List[ExpectationResult] = field(default_factory=list)
    row_count: int = 0
    report_path: Optional[str] = None
    gcs_report_uri: Optional[str] = None

    @property
    def summary(self) -> str:
        status = "PASS" if self.passed_threshold else "FAIL"
        return (
            f"[{status}] {self.dataset_name}/{self.suite_name} "
            f"DQS={self.data_quality_score:.1%} "
            f"(threshold={self.threshold:.1%}) "
            f"passed={self.passed_expectations}/{self.total_expectations}"
        )


# ---------------------------------------------------------------------------
# Core DataQualityFramework class
# ---------------------------------------------------------------------------


class DataQualityFramework:
    """
    Enterprise Data Platform data quality validation engine.

    Usage:
        config = DataQualityConfig(
            suites_dir="/path/to/expectations",
            alert=AlertConfig(slack_webhook_url="https://hooks.slack.com/..."),
            bq_monitoring=BigQueryMonitoringConfig(project="my-proj", dataset="monitoring"),
        )
        dqf = DataQualityFramework(config)

        suite_cfg = SuiteConfig(
            suite_name="payments_silver_suite",
            layer=DataLayer.SILVER,
            dataset_name="payments",
        )
        result = dqf.validate(df=payments_df, suite_config=suite_cfg)
        if not result.passed_threshold:
            raise ValueError(result.summary)
    """

    def __init__(self, config: DataQualityConfig) -> None:
        self._config = config
        self._bq_client: Optional[bigquery.Client] = None
        self._suite_cache: Dict[str, Dict[str, Any]] = {}

        Path(config.reports_output_dir).mkdir(parents=True, exist_ok=True)

        if config.bq_monitoring:
            self._bq_client = bigquery.Client(project=config.bq_monitoring.project)
            self._ensure_bq_table_exists()

        logger.info(
            "DataQualityFramework initialized | suites_dir=%s | bq_monitoring=%s",
            config.suites_dir,
            config.bq_monitoring.full_table_id if config.bq_monitoring else "disabled",
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate(
        self,
        df: pd.DataFrame,
        suite_config: SuiteConfig,
        run_id: Optional[str] = None,
    ) -> ValidationResult:
        """
        Validate a Pandas DataFrame against the named expectation suite.

        Args:
            df: The DataFrame to validate.
            suite_config: Suite configuration specifying layer, threshold, and suite name.
            run_id: Optional caller-supplied run identifier (UUID generated if omitted).

        Returns:
            ValidationResult with full per-expectation breakdown.

        Raises:
            ValueError: If raise_on_failure is True and quality drops below threshold.
        """
        run_id = run_id or str(uuid.uuid4())
        evaluated_at = datetime.now(timezone.utc).isoformat()

        logger.info(
            "Starting validation | run_id=%s | suite=%s | dataset=%s | rows=%d",
            run_id,
            suite_config.suite_name,
            suite_config.dataset_name,
            len(df),
        )

        suite = self._load_suite(suite_config)
        expectation_results = self._evaluate_suite(df, suite)

        total = len(expectation_results)
        passed = sum(1 for r in expectation_results if r.success)
        failed = total - passed
        dqs = passed / total if total > 0 else 0.0
        passed_threshold = dqs >= suite_config.threshold
        severity = self._compute_severity(dqs, suite_config.threshold)

        result = ValidationResult(
            run_id=run_id,
            dataset_name=suite_config.dataset_name,
            suite_name=suite_config.suite_name,
            layer=suite_config.layer,
            evaluated_at_utc=evaluated_at,
            total_expectations=total,
            passed_expectations=passed,
            failed_expectations=failed,
            data_quality_score=dqs,
            threshold=suite_config.threshold,
            passed_threshold=passed_threshold,
            severity=severity,
            expectation_results=expectation_results,
            row_count=len(df),
        )

        logger.info("Validation complete: %s", result.summary)
        self._finalize_result(result)
        return result

    def validate_spark_dataframe(
        self,
        spark_df: Any,  # pyspark.sql.DataFrame
        suite_config: SuiteConfig,
        sample_fraction: float = 0.10,
        run_id: Optional[str] = None,
    ) -> ValidationResult:
        """
        DEPRECATED — driver-sampling path. Kept for backwards compatibility.

        Pulls a sample to driver memory via toPandas(): at TB scale even 10%
        is enormous on one machine, and sampling statistically misses rare
        defects. Use validate_spark_distributed() instead.
        """
        logger.warning(
            "validate_spark_dataframe uses driver sampling (toPandas) — "
            "switch to validate_spark_distributed for full-scan in-cluster "
            "validation. suite=%s",
            suite_config.suite_name,
        )
        sampled_df = spark_df.sample(fraction=sample_fraction, seed=42).toPandas()
        return self.validate(sampled_df, suite_config, run_id=run_id)

    def validate_spark_distributed(
        self,
        spark_df: Any,  # pyspark.sql.DataFrame
        suite_config: SuiteConfig,
        run_id: Optional[str] = None,
    ) -> ValidationResult:
        """
        Full-table validation IN Spark — no data ever reaches the driver
        except the single aggregate row of expectation outcomes.

        The whole suite compiles into ONE df.agg(...) pass regardless of how
        many expectations it contains (see src/validation/distributed_dq.py).
        Reports/alerts/DQS-persistence behave identically to validate().
        """
        from src.validation.distributed_dq import DistributedExpectationEngine

        run_id = run_id or str(uuid.uuid4())
        evaluated_at = datetime.now(timezone.utc).isoformat()

        logger.info(
            "Starting DISTRIBUTED validation | run_id=%s | suite=%s | dataset=%s",
            run_id,
            suite_config.suite_name,
            suite_config.dataset_name,
        )

        suite = self._load_suite(suite_config)
        engine = DistributedExpectationEngine()
        expectation_results = engine.evaluate(spark_df, suite)

        total = len(expectation_results)
        passed = sum(1 for r in expectation_results if r.success)
        failed = total - passed
        dqs = passed / total if total > 0 else 0.0
        passed_threshold = dqs >= suite_config.threshold
        severity = self._compute_severity(dqs, suite_config.threshold)

        result = ValidationResult(
            run_id=run_id,
            dataset_name=suite_config.dataset_name,
            suite_name=suite_config.suite_name,
            layer=suite_config.layer,
            evaluated_at_utc=evaluated_at,
            total_expectations=total,
            passed_expectations=passed,
            failed_expectations=failed,
            data_quality_score=dqs,
            threshold=suite_config.threshold,
            passed_threshold=passed_threshold,
            severity=severity,
            expectation_results=expectation_results,
            row_count=-1,  # resolved below via row-count expectation if present
        )

        # Prefer an explicit row-count expectation's observed value; else ask once.
        rc = next(
            (
                r.observed_value
                for r in expectation_results
                if r.expectation_type == "expect_table_row_count_to_be_between"
            ),
            None,
        )
        result.row_count = int(rc) if isinstance(rc, int) else int(spark_df.count())

        logger.info("Distributed validation complete: %s", result.summary)
        self._finalize_result(result)
        return result

    def _finalize_result(self, result: ValidationResult) -> None:
        """Shared post-validation side effects: report → BQ → alerts → raise."""
        report_path = self._generate_report(result)
        result.report_path = report_path

        if self._bq_client:
            self._write_dqs_to_bigquery(result)

        if not result.passed_threshold:
            self._send_alerts(result)
            if self._config.raise_on_failure and result.severity == AlertSeverity.P1:
                raise DataQualityError(result)

    def load_all_suites(self) -> List[str]:
        """Return the names of all available expectation suites in suites_dir."""
        suites_path = Path(self._config.suites_dir)
        return [p.stem for p in suites_path.glob("*.json")]

    def get_dqs_history(
        self,
        dataset_name: str,
        days: int = 30,
    ) -> Optional[pd.DataFrame]:
        """
        Query historical DQS scores for a dataset from BigQuery.
        Returns a DataFrame with columns: evaluated_at_utc, data_quality_score,
        passed_threshold, failed_expectations, row_count.
        """
        if not self._bq_client or not self._config.bq_monitoring:
            logger.warning("BigQuery monitoring not configured; cannot retrieve DQS history.")
            return None

        table_id = self._config.bq_monitoring.full_table_id
        days = int(days)  # guard against f-string SQL injection via non-int input
        query = f"""
            SELECT
                evaluated_at_utc,
                data_quality_score,
                passed_threshold,
                failed_expectations,
                row_count,
                suite_name
            FROM `{table_id}`
            WHERE
                dataset_name = @dataset_name
                AND TIMESTAMP(evaluated_at_utc) >=
                    TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
            ORDER BY evaluated_at_utc DESC
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("dataset_name", "STRING", dataset_name)]
        )
        logger.info("Fetching DQS history | dataset=%s | days=%d", dataset_name, days)
        return self._bq_client.query(query, job_config=job_config).to_dataframe()

    # ------------------------------------------------------------------
    # Suite loading
    # ------------------------------------------------------------------

    def _load_suite(self, suite_config: SuiteConfig) -> Dict[str, Any]:
        """
        Load an expectation suite from disk (JSON file).
        Suites are cached in memory after first load.
        """
        suite_name = suite_config.suite_name

        if suite_name in self._suite_cache:
            return self._suite_cache[suite_name]

        # Determine file path
        if suite_config.suite_path:
            suite_path = Path(suite_config.suite_path)
        else:
            suite_path = Path(self._config.suites_dir) / f"{suite_name}.json"

        if not suite_path.exists():
            raise FileNotFoundError(
                f"Expectation suite not found: {suite_path}. "
                f"Available suites: {self.load_all_suites()}"
            )

        with suite_path.open("r", encoding="utf-8") as fh:
            suite = json.load(fh)

        self._suite_cache[suite_name] = suite
        logger.info(
            "Loaded expectation suite: %s (%d expectations)",
            suite_name,
            len(suite.get("expectations", [])),
        )
        return suite

    # ------------------------------------------------------------------
    # Expectation evaluation engine
    # ------------------------------------------------------------------

    def _evaluate_suite(
        self,
        df: pd.DataFrame,
        suite: Dict[str, Any],
    ) -> List[ExpectationResult]:
        """
        Evaluate all expectations in the suite against the DataFrame.
        Returns a list of ExpectationResult objects, one per expectation.
        """
        results: List[ExpectationResult] = []
        expectations = suite.get("expectations", [])

        if not expectations:
            logger.warning("Suite contains no expectations.")
            return results

        for expectation in expectations:
            exp_type: str = expectation.get("expectation_type", "")
            kwargs: Dict[str, Any] = expectation.get("kwargs", {})

            try:
                result = self._dispatch_expectation(df, exp_type, kwargs)
                results.append(result)
            except Exception as exc:
                logger.warning(
                    "Expectation evaluation error | type=%s | kwargs=%s | error=%s",
                    exp_type,
                    kwargs,
                    exc,
                )
                # Treat unevaluable expectations as failures
                results.append(
                    ExpectationResult(
                        expectation_type=exp_type,
                        column=kwargs.get("column"),
                        success=False,
                        observed_value=None,
                        element_count=len(df),
                        unexpected_count=None,
                        unexpected_percent=None,
                        details={"evaluation_error": str(exc)},
                    )
                )

        return results

    def _dispatch_expectation(
        self,
        df: pd.DataFrame,
        expectation_type: str,
        kwargs: Dict[str, Any],
    ) -> ExpectationResult:
        """Route an expectation type to the corresponding evaluation method."""
        handlers: Dict[str, Callable] = {
            "expect_column_values_to_not_be_null": self._eval_not_null,
            "expect_column_values_to_be_unique": self._eval_unique,
            "expect_column_values_to_be_in_set": self._eval_in_set,
            "expect_column_values_to_be_between": self._eval_between,
            "expect_column_values_to_match_regex": self._eval_regex,
            "expect_table_row_count_to_be_between": self._eval_row_count_between,
            "expect_table_columns_to_match_ordered_list": self._eval_columns_ordered,
            "expect_column_to_exist": self._eval_column_exists,
            "expect_column_values_to_be_of_type": self._eval_column_type,
            "expect_column_mean_to_be_between": self._eval_mean_between,
            "expect_column_sum_to_be_between": self._eval_sum_between,
        }

        handler = handlers.get(expectation_type)
        if not handler:
            raise NotImplementedError(f"Expectation type not implemented: {expectation_type}")

        return handler(df, kwargs)

    # ------ Individual expectation evaluators ------

    def _eval_not_null(self, df: pd.DataFrame, kwargs: Dict) -> ExpectationResult:
        col = kwargs["column"]
        mostly = kwargs.get("mostly", 1.0)
        null_count = int(df[col].isna().sum())
        total = len(df)
        null_pct = null_count / total if total > 0 else 0.0
        non_null_pct = 1.0 - null_pct
        return ExpectationResult(
            expectation_type="expect_column_values_to_not_be_null",
            column=col,
            success=non_null_pct >= mostly,
            observed_value=non_null_pct,
            element_count=total,
            unexpected_count=null_count,
            unexpected_percent=null_pct * 100,
        )

    def _eval_unique(self, df: pd.DataFrame, kwargs: Dict) -> ExpectationResult:
        col = kwargs["column"]
        mostly = kwargs.get("mostly", 1.0)
        total = len(df)
        dup_count = int(df[col].duplicated().sum())
        unique_pct = (total - dup_count) / total if total > 0 else 1.0
        return ExpectationResult(
            expectation_type="expect_column_values_to_be_unique",
            column=col,
            success=unique_pct >= mostly,
            observed_value=unique_pct,
            element_count=total,
            unexpected_count=dup_count,
            unexpected_percent=(dup_count / total * 100) if total > 0 else 0.0,
        )

    def _eval_in_set(self, df: pd.DataFrame, kwargs: Dict) -> ExpectationResult:
        col = kwargs["column"]
        value_set = set(kwargs["value_set"])
        mostly = kwargs.get("mostly", 1.0)
        non_null = df[col].notna()  # GE semantics: nulls excluded
        total = int(non_null.sum())
        invalid_count = int((~df.loc[non_null, col].isin(value_set)).sum())
        valid_pct = (total - invalid_count) / total if total > 0 else 1.0
        return ExpectationResult(
            expectation_type="expect_column_values_to_be_in_set",
            column=col,
            success=valid_pct >= mostly,
            observed_value=list(df[col].dropna().unique()[:20]),
            element_count=total,
            unexpected_count=invalid_count,
            unexpected_percent=(invalid_count / total * 100) if total > 0 else 0.0,
        )

    def _eval_between(self, df: pd.DataFrame, kwargs: Dict) -> ExpectationResult:
        col = kwargs["column"]
        min_val = kwargs.get("min_value")
        max_val = kwargs.get("max_value")
        mostly = kwargs.get("mostly", 1.0)
        # GE semantics: nulls are excluded from evaluation entirely
        non_null = df[col].notna()
        total = int(non_null.sum())
        series = pd.to_numeric(df.loc[non_null, col], errors="coerce")
        mask = pd.Series(True, index=series.index)
        if min_val is not None:
            mask &= series >= min_val
        if max_val is not None:
            mask &= series <= max_val
        invalid_count = int((~mask).sum())
        valid_pct = (total - invalid_count) / total if total > 0 else 1.0
        return ExpectationResult(
            expectation_type="expect_column_values_to_be_between",
            column=col,
            success=valid_pct >= mostly,
            observed_value={
                "min": float(series.min()) if len(series) else None,
                "max": float(series.max()) if len(series) else None,
            },
            element_count=total,
            unexpected_count=invalid_count,
            unexpected_percent=(invalid_count / total * 100) if total > 0 else 0.0,
        )

    def _eval_regex(self, df: pd.DataFrame, kwargs: Dict) -> ExpectationResult:
        col = kwargs["column"]
        regex = kwargs["regex"]
        mostly = kwargs.get("mostly", 1.0)
        non_null_str = df[col].dropna().astype(str)  # GE semantics: nulls excluded
        total = len(non_null_str)
        match_mask = non_null_str.str.match(regex, na=False)
        invalid_count = int((~match_mask).sum())
        valid_pct = (total - invalid_count) / total if total > 0 else 1.0
        return ExpectationResult(
            expectation_type="expect_column_values_to_match_regex",
            column=col,
            success=valid_pct >= mostly,
            observed_value={"regex": regex, "match_rate": valid_pct},
            element_count=total,
            unexpected_count=invalid_count,
            unexpected_percent=(invalid_count / total * 100) if total > 0 else 0.0,
        )

    def _eval_row_count_between(self, df: pd.DataFrame, kwargs: Dict) -> ExpectationResult:
        min_val = kwargs.get("min_value", 0)
        max_val = kwargs.get("max_value")
        row_count = len(df)
        in_range = row_count >= min_val and (max_val is None or row_count <= max_val)
        return ExpectationResult(
            expectation_type="expect_table_row_count_to_be_between",
            column=None,
            success=in_range,
            observed_value=row_count,
            element_count=row_count,
            unexpected_count=0 if in_range else 1,
            unexpected_percent=0.0 if in_range else 100.0,
        )

    def _eval_columns_ordered(self, df: pd.DataFrame, kwargs: Dict) -> ExpectationResult:
        expected = kwargs["column_list"]
        actual = list(df.columns)
        success = actual == expected
        return ExpectationResult(
            expectation_type="expect_table_columns_to_match_ordered_list",
            column=None,
            success=success,
            observed_value=actual,
            element_count=len(actual),
            unexpected_count=0 if success else 1,
            unexpected_percent=0.0 if success else 100.0,
            details={"expected": expected},
        )

    def _eval_column_exists(self, df: pd.DataFrame, kwargs: Dict) -> ExpectationResult:
        col = kwargs["column"]
        exists = col in df.columns
        return ExpectationResult(
            expectation_type="expect_column_to_exist",
            column=col,
            success=exists,
            observed_value=list(df.columns),
            element_count=len(df),
            unexpected_count=0 if exists else 1,
            unexpected_percent=0.0,
        )

    def _eval_column_type(self, df: pd.DataFrame, kwargs: Dict) -> ExpectationResult:
        col = kwargs["column"]
        expected_type = kwargs["type_"]
        actual_dtype = str(df[col].dtype)
        success = expected_type.lower() in actual_dtype.lower()
        return ExpectationResult(
            expectation_type="expect_column_values_to_be_of_type",
            column=col,
            success=success,
            observed_value=actual_dtype,
            element_count=len(df),
            unexpected_count=0 if success else 1,
            unexpected_percent=0.0,
            details={"expected_type": expected_type},
        )

    def _eval_mean_between(self, df: pd.DataFrame, kwargs: Dict) -> ExpectationResult:
        col = kwargs["column"]
        min_val = kwargs.get("min_value")
        max_val = kwargs.get("max_value")
        mean_val = float(pd.to_numeric(df[col], errors="coerce").mean())
        in_range = (min_val is None or mean_val >= min_val) and (
            max_val is None or mean_val <= max_val
        )
        return ExpectationResult(
            expectation_type="expect_column_mean_to_be_between",
            column=col,
            success=in_range,
            observed_value=mean_val,
            element_count=len(df),
            unexpected_count=0 if in_range else 1,
            unexpected_percent=0.0,
        )

    def _eval_sum_between(self, df: pd.DataFrame, kwargs: Dict) -> ExpectationResult:
        col = kwargs["column"]
        min_val = kwargs.get("min_value")
        max_val = kwargs.get("max_value")
        sum_val = float(pd.to_numeric(df[col], errors="coerce").sum())
        in_range = (min_val is None or sum_val >= min_val) and (
            max_val is None or sum_val <= max_val
        )
        return ExpectationResult(
            expectation_type="expect_column_sum_to_be_between",
            column=col,
            success=in_range,
            observed_value=sum_val,
            element_count=len(df),
            unexpected_count=0 if in_range else 1,
            unexpected_percent=0.0,
        )

    # ------------------------------------------------------------------
    # Severity classification
    # ------------------------------------------------------------------

    def _compute_severity(self, dqs: float, threshold: float) -> AlertSeverity:
        """
        Map DQS shortfall to a severity level.
          - DQS >= threshold                    : no alert (return P3 for logging only)
          - threshold - 0.05 <= DQS < threshold : P2 (high)
          - DQS < threshold - 0.05              : P1 (critical)
        """
        if dqs >= threshold:
            return AlertSeverity.P3
        elif dqs >= threshold - 0.05:
            return AlertSeverity.P2
        else:
            return AlertSeverity.P1

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def _generate_report(self, result: ValidationResult) -> str:
        """
        Write a JSON validation report locally, render an HTML summary alongside
        it, and (if configured) copy both to GCS for durable storage.
        Returns the local JSON file path; the GCS URI lands in result.details
        and the BigQuery monitoring row.
        """
        output_dir = Path(self._config.reports_output_dir)
        base_filename = f"{result.dataset_name}_{result.suite_name}_{result.run_id[:8]}"
        json_path = output_dir / f"{base_filename}.json"
        html_path = output_dir / f"{base_filename}.html"

        report_data = {
            "run_id": result.run_id,
            "dataset_name": result.dataset_name,
            "suite_name": result.suite_name,
            "layer": result.layer.value,
            "evaluated_at_utc": result.evaluated_at_utc,
            "data_quality_score": result.data_quality_score,
            "threshold": result.threshold,
            "passed_threshold": result.passed_threshold,
            "severity": result.severity.value,
            "row_count": result.row_count,
            "total_expectations": result.total_expectations,
            "passed_expectations": result.passed_expectations,
            "failed_expectations": result.failed_expectations,
            "gcs_report_uri": result.gcs_report_uri,
            "expectation_results": [
                {
                    "expectation_type": r.expectation_type,
                    "column": r.column,
                    "success": r.success,
                    "observed_value": str(r.observed_value),
                    "unexpected_count": r.unexpected_count,
                    "unexpected_percent": r.unexpected_percent,
                }
                for r in result.expectation_results
            ],
        }

        with json_path.open("w", encoding="utf-8") as fh:
            json.dump(report_data, fh, indent=2)

        html_path.write_text(self._render_html_report(result), encoding="utf-8")

        logger.info("Validation reports written | json=%s | html=%s", json_path, html_path)

        if self._config.reports_gcs_bucket:
            gcs_uri = self._upload_reports_to_gcs([json_path, html_path], result.run_id)
            if gcs_uri:
                result.gcs_report_uri = gcs_uri

        return str(json_path)

    @staticmethod
    def _render_html_report(result: ValidationResult) -> str:
        """Render a minimal standalone HTML validation summary."""
        status_color = "#1a7f37" if result.passed_threshold else "#cf222e"

        def _fmt_unexpected_pct(value: Optional[float]) -> str:
            return "" if value is None else f"{round(value, 2)}"

        rows_html = ""
        for r in result.expectation_results:
            badge = (
                '<span style="color:#1a7f37">PASS</span>'
                if r.success
                else '<span style="color:#cf222e">FAIL</span>'
            )
            rows_html += (
                "<tr>"
                f"<td>{r.expectation_type}</td>"
                f"<td>{r.column or '—'}</td>"
                f"<td>{badge}</td>"
                f"<td>{str(r.observed_value)[:80]}</td>"
                f"<td>{r.unexpected_count}</td>"
                f"<td>{_fmt_unexpected_pct(r.unexpected_percent)}%</td>"
                "</tr>"
            )

        return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>DQ Report — {result.dataset_name}/{result.suite_name}</title>
<style>
body {{ font-family: -apple-system, sans-serif; margin: 2rem; }}
table {{ border-collapse: collapse; width: 100%; margin-top: 1rem; }}
th, td {{ border: 1px solid #d0d7de; padding: 6px 10px; text-align: left; font-size: 14px; }}
th {{ background: #f6f8fa; }}
h1 span {{ color: {status_color}; }}
</style></head>
<body>
<h1>Data Quality Report — <span>{'PASS' if result.passed_threshold else 'FAIL'}</span></h1>
<p>
<b>Dataset:</b> {result.dataset_name} &nbsp;|&nbsp;
<b>Suite:</b> {result.suite_name} &nbsp;|&nbsp;
<b>Layer:</b> {result.layer.value.upper()} &nbsp;|&nbsp;
<b>DQS:</b> {result.data_quality_score:.2%} (threshold {result.threshold:.2%})<br>
<b>Rows:</b> {result.row_count:,} &nbsp;|&nbsp;
<b>Expectations:</b> {result.passed_expectations}/{result.total_expectations} passed &nbsp;|&nbsp;
<b>Run ID:</b> <code>{result.run_id}</code><br>
<b>Evaluated:</b> {result.evaluated_at_utc}
</p>
<table>
<tr><th>Expectation</th><th>Column</th><th>Status</th><th>Observed</th><th>Unexpected</th><th>%</th></tr>
{rows_html}
</table>
</body>
</html>"""

    def _upload_reports_to_gcs(self, local_paths: List[Path], run_id: str) -> Optional[str]:
        """Upload report artifacts to GCS under runs/{run_id}/. Returns the folder URI."""
        try:
            from google.cloud import storage  # type: ignore[attr-defined]

            client = storage.Client()
            bucket = client.bucket(self._config.reports_gcs_bucket)
            for lp in local_paths:
                blob = bucket.blob(f"runs/{run_id}/{lp.name}")
                blob.upload_from_filename(str(lp))
            uri = f"gs://{self._config.reports_gcs_bucket}/runs/{run_id}/"
            logger.info("Reports uploaded to GCS: %s", uri)
            return uri
        except Exception as exc:
            logger.error("Failed to upload reports to GCS: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Alerting
    # ------------------------------------------------------------------

    def _send_alerts(self, result: ValidationResult) -> None:
        """Dispatch alerts to configured channels based on severity."""
        alert_cfg = self._config.alert

        if (
            alert_cfg.slack_webhook_url
            and result.severity.rank >= alert_cfg.slack_min_severity.rank
        ):
            self._send_slack_alert(result, alert_cfg.slack_webhook_url)

        if alert_cfg.pagerduty_routing_key and result.severity == AlertSeverity.P1:
            self._send_pagerduty_alert(result, alert_cfg.pagerduty_routing_key)

    def _send_slack_alert(self, result: ValidationResult, webhook_url: str) -> None:
        """Post a Slack message summarizing the quality failure."""
        severity_emoji = {
            "P1": ":red_circle:",
            "P2": ":large_yellow_circle:",
            "P3": ":white_circle:",
        }
        emoji = severity_emoji.get(result.severity.value, ":question:")

        failed_expectations_text = "\n".join(
            f"  • `{r.expectation_type}` on `{r.column or 'table'}` "
            f"— unexpected: {r.unexpected_count} ({r.unexpected_percent:.1f}%)"
            for r in result.expectation_results
            if not r.success
        )[
            :2000
        ]  # Slack has a 3000-char limit per block

        payload = {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"{emoji} Data Quality Failure — {result.severity.value}",
                    },
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Dataset:*\n{result.dataset_name}"},
                        {"type": "mrkdwn", "text": f"*Suite:*\n{result.suite_name}"},
                        {"type": "mrkdwn", "text": f"*Layer:*\n{result.layer.value.upper()}"},
                        {
                            "type": "mrkdwn",
                            "text": (
                                f"*DQS:*\n{result.data_quality_score:.1%} "
                                f"(threshold: {result.threshold:.1%})"
                            ),
                        },
                        {"type": "mrkdwn", "text": f"*Rows Validated:*\n{result.row_count:,}"},
                        {"type": "mrkdwn", "text": f"*Run ID:*\n`{result.run_id}`"},
                    ],
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f"*Failed Expectations ({result.failed_expectations}):*"
                            f"\n{failed_expectations_text}"
                        ),
                    },
                },
            ]
        }

        try:
            response = requests.post(webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            logger.info("Slack alert sent for run_id=%s", result.run_id)
        except Exception as exc:
            logger.error("Failed to send Slack alert: %s", exc)

    def _send_pagerduty_alert(self, result: ValidationResult, routing_key: str) -> None:
        """Trigger a PagerDuty incident for a P1 data quality failure."""
        payload = {
            "routing_key": routing_key,
            "event_action": "trigger",
            "dedup_key": f"dq-{result.dataset_name}-{result.suite_name}",
            "payload": {
                "summary": (
                    f"[DATA QUALITY P1] {result.dataset_name}/{result.suite_name} "
                    f"DQS={result.data_quality_score:.1%} below threshold {result.threshold:.1%}"
                ),
                "severity": "critical",
                "source": "enterprise-data-platform",
                "component": result.dataset_name,
                "group": result.layer.value,
                "class": "data_quality",
                "custom_details": {
                    "run_id": result.run_id,
                    "layer": result.layer.value,
                    "data_quality_score": f"{result.data_quality_score:.4f}",
                    "failed_expectations": result.failed_expectations,
                    "total_expectations": result.total_expectations,
                    "row_count": result.row_count,
                    "report_path": result.report_path,
                },
            },
        }

        try:
            response = requests.post(
                "https://events.pagerduty.com/v2/enqueue",
                json=payload,
                timeout=10,
            )
            response.raise_for_status()
            logger.info(
                "PagerDuty incident triggered | dedup_key=%s",
                payload["dedup_key"],
            )
        except Exception as exc:
            logger.error("Failed to trigger PagerDuty incident: %s", exc)

    # ------------------------------------------------------------------
    # BigQuery monitoring
    # ------------------------------------------------------------------

    def _ensure_bq_table_exists(self) -> None:
        """Create the BigQuery DQS tracking table if it does not exist."""
        cfg = self._config.bq_monitoring
        if cfg is None or self._bq_client is None:
            return
        schema = [
            bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("dataset_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("suite_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("layer", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("evaluated_at_utc", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("data_quality_score", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("threshold", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("passed_threshold", "BOOL", mode="REQUIRED"),
            bigquery.SchemaField("severity", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("total_expectations", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("passed_expectations", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("failed_expectations", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("row_count", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("report_path", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("gcs_report_uri", "STRING", mode="NULLABLE"),
        ]
        table_ref = bigquery.Table(cfg.full_table_id, schema=schema)
        table_ref.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="evaluated_at_utc",
        )
        try:
            self._bq_client.create_table(table_ref, exists_ok=True)
            logger.info("BigQuery DQS table ready: %s", cfg.full_table_id)
        except Exception as exc:
            logger.error("Failed to create BigQuery DQS table: %s", exc)

    def _write_dqs_to_bigquery(self, result: ValidationResult) -> None:
        """Insert a single DQS score row into the BigQuery monitoring table."""
        cfg = self._config.bq_monitoring
        if cfg is None or self._bq_client is None:
            return
        row = {
            "run_id": result.run_id,
            "dataset_name": result.dataset_name,
            "suite_name": result.suite_name,
            "layer": result.layer.value,
            "evaluated_at_utc": result.evaluated_at_utc,
            "data_quality_score": result.data_quality_score,
            "threshold": result.threshold,
            "passed_threshold": result.passed_threshold,
            "severity": result.severity.value,
            "total_expectations": result.total_expectations,
            "passed_expectations": result.passed_expectations,
            "failed_expectations": result.failed_expectations,
            "row_count": result.row_count,
            "report_path": result.report_path,
            "gcs_report_uri": result.gcs_report_uri,
        }

        errors = self._bq_client.insert_rows_json(cfg.full_table_id, [row])
        if errors:
            logger.error(
                "Failed to write DQS score to BigQuery | run_id=%s | errors=%s",
                result.run_id,
                errors,
            )
        else:
            logger.info(
                "DQS score written to BigQuery | run_id=%s | dqs=%.4f",
                result.run_id,
                result.data_quality_score,
            )


# ---------------------------------------------------------------------------
# Custom exception
# ---------------------------------------------------------------------------


class DataQualityError(Exception):
    """Raised when a P1 data quality failure occurs and raise_on_failure=True."""

    def __init__(self, result: ValidationResult) -> None:
        self.result = result
        super().__init__(result.summary)


# ---------------------------------------------------------------------------
# Factory helper
# ---------------------------------------------------------------------------


def build_framework_from_env() -> DataQualityFramework:
    """
    Build a DataQualityFramework from environment variables.

    Required:
        DQ_SUITES_DIR          — path to expectation suite JSON files
    Optional:
        DQ_SLACK_WEBHOOK_URL   — Slack incoming webhook for alerts
        DQ_PAGERDUTY_KEY       — PagerDuty routing key for P1 incidents
        DQ_BQ_PROJECT          — GCP project for BigQuery monitoring
        DQ_BQ_DATASET          — BigQuery dataset for DQS tracking
        DQ_RAISE_ON_FAILURE    — "true"/"false" (default: "true")
        DQ_REPORTS_DIR         — local path for validation reports
        DQ_REPORTS_GCS_BUCKET  — GCS bucket for durable report storage
    """
    suites_dir = os.environ["DQ_SUITES_DIR"]
    bq_project = os.getenv("DQ_BQ_PROJECT")
    bq_dataset = os.getenv("DQ_BQ_DATASET")

    bq_cfg = None
    if bq_project and bq_dataset:
        bq_cfg = BigQueryMonitoringConfig(project=bq_project, dataset=bq_dataset)

    config = DataQualityConfig(
        suites_dir=suites_dir,
        alert=AlertConfig(
            slack_webhook_url=os.getenv("DQ_SLACK_WEBHOOK_URL"),
            pagerduty_routing_key=os.getenv("DQ_PAGERDUTY_KEY"),
        ),
        bq_monitoring=bq_cfg,
        raise_on_failure=os.getenv("DQ_RAISE_ON_FAILURE", "true").lower() == "true",
        reports_output_dir=os.getenv("DQ_REPORTS_DIR", "/tmp/dq_reports"),
        reports_gcs_bucket=os.getenv("DQ_REPORTS_GCS_BUCKET") or None,
    )
    return DataQualityFramework(config)
