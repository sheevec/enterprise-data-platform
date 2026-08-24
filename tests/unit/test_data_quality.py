"""Unit tests for src/validation/data_quality.py (offline — no GCP required)."""

import json

import pandas as pd
import pytest

from src.validation.data_quality import (
    AlertSeverity,
    DataLayer,
    DataQualityConfig,
    DataQualityError,
    DataQualityFramework,
    SuiteConfig,
)

SUITE = {
    "expectations": [
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "order_id"},
        },
        {"expectation_type": "expect_column_values_to_be_unique", "kwargs": {"column": "order_id"}},
        {
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {"column": "status", "value_set": ["created", "shipped"]},
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "amount", "min_value": 0, "max_value": 1000},
        },
        {
            "expectation_type": "expect_table_row_count_to_be_between",
            "kwargs": {"min_value": 1, "max_value": 10_000},
        },
    ]
}


@pytest.fixture()
def framework(tmp_path):
    suites_dir = tmp_path / "suites"
    suites_dir.mkdir()
    (suites_dir / "orders_suite.json").write_text(json.dumps(SUITE))
    reports_dir = tmp_path / "reports"

    config = DataQualityConfig(
        suites_dir=str(suites_dir),
        reports_output_dir=str(reports_dir),
        raise_on_failure=False,
    )
    return DataQualityFramework(config)


def _suite_config(layer=DataLayer.SILVER, **overrides):
    defaults = dict(
        suite_name="orders_suite",
        layer=layer,
        dataset_name="orders",
    )
    defaults.update(overrides)
    return SuiteConfig(**defaults)


VALID_DF = pd.DataFrame(
    {
        "order_id": ["o-1", "o-2", "o-3"],
        "status": ["created", "shipped", "created"],
        "amount": [10.0, 25.5, 999.0],
    }
)


class TestValidatePasses:
    def test_clean_dataframe_scores_full_marks(self, framework):
        result = framework.validate(VALID_DF.copy(), _suite_config())

        assert result.passed_threshold
        assert result.data_quality_score == 1.0
        assert result.total_expectations == len(SUITE["expectations"])
        assert result.report_path is not None

    def test_reports_written(self, framework):
        result = framework.validate(VALID_DF.copy(), _suite_config())

        report_file = framework._config.reports_output_dir
        assert result.report_path.startswith(report_file)
        html_sibling = result.report_path.replace(".json", ".html")
        import os

        assert os.path.exists(html_sibling)

    def test_severity_ok_when_passing(self, framework):
        result = framework.validate(VALID_DF.copy(), _suite_config())
        assert result.severity == AlertSeverity.P3  # no alert severity


class TestValidateFails:
    def _dirty_df(self):
        df = VALID_DF.copy()
        df.loc[0, "order_id"] = None  # not-null violation
        df.loc[2, "order_id"] = "o-2"  # duplicate
        df.loc[1, "status"] = "teleported"  # set membership violation
        df.loc[2, "amount"] = -50  # range violation
        return df

    def test_dqs_below_threshold_detected(self, framework):
        result = framework.validate(self._dirty_df(), _suite_config())

        assert not result.passed_threshold
        assert result.failed_expectations >= 3
        failed_types = {r.expectation_type for r in result.expectation_results if not r.success}
        assert "expect_column_values_to_not_be_null" in failed_types
        assert "expect_column_values_to_be_in_set" in failed_types

    def test_p1_failure_raises_when_configured(self, tmp_path):
        suites_dir = tmp_path / "s"
        suites_dir.mkdir()
        (suites_dir / "orders_suite.json").write_text(json.dumps(SUITE))

        strict = DataQualityFramework(
            DataQualityConfig(suites_dir=str(suites_dir), raise_on_failure=True)
        )
        with pytest.raises(DataQualityError) as exc_info:
            strict.validate(
                pd.DataFrame({"order_id": [None], "status": ["bad"], "amount": [-1]}),
                _suite_config(),
            )
        assert exc_info.value.result.severity == AlertSeverity.P1

    def test_threshold_override_respected(self, framework):
        # Perfect data passes any threshold
        result = framework.validate(VALID_DF.copy(), _suite_config(threshold_override=1.0))
        assert result.threshold == 1.0
        assert result.passed_threshold


class TestSeverityModel:
    def test_rank_ordering(self):
        assert AlertSeverity.P3.rank < AlertSeverity.P2.rank < AlertSeverity.P1.rank

    def test_compute_severity_bands(self, framework):
        threshold = 0.95
        assert framework._compute_severity(0.96, threshold) == AlertSeverity.P3
        assert framework._compute_severity(0.93, threshold) == AlertSeverity.P2
        assert framework._compute_severity(0.80, threshold) == AlertSeverity.P1


class TestSuiteLoading:
    def test_missing_suite_lists_available(self, framework):
        with pytest.raises(FileNotFoundError, match="Available suites"):
            framework.validate(VALID_DF, _suite_config(suite_name="does_not_exist"))

    def test_load_all_suites(self, framework):
        assert framework.load_all_suites() == ["orders_suite"]

    def test_no_expectations_is_zero_score(self, framework):
        from pathlib import Path

        suites_path = Path(framework._config.suites_dir)
        (suites_path / "empty_suite.json").write_text(json.dumps({"expectations": []}))

        cfg = SuiteConfig(suite_name="empty_suite", layer=DataLayer.GOLD, dataset_name="x")
        result = framework.validate(VALID_DF, cfg)
        assert not result.passed_threshold
        assert result.data_quality_score == 0.0
