"""Tests for src/validation/distributed_dq.py — Spark-native, single-pass DQ."""

import pytest

from src.validation.distributed_dq import (
    DistributedExpectationEngine,
    ViolationExtractor,
    anchor_pattern,
)

spark = pytest.importorskip("pyspark.sql")

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
            "kwargs": {"min_value": 1, "max_value": 10000},
        },
        {"expectation_type": "expect_column_to_exist", "kwargs": {"column": "amount"}},
        {
            "expectation_type": "expect_table_columns_to_match_ordered_list",
            "kwargs": {"column_list": ["order_id", "status", "amount"]},
        },
        {
            "expectation_type": "expect_column_mean_to_be_between",
            "kwargs": {"column": "amount", "min_value": 0, "max_value": 500},
        },
        {
            "expectation_type": "expect_column_sum_to_be_between",
            "kwargs": {"column": "amount", "min_value": 0, "max_value": 100000},
        },
        {
            "expectation_type": "expect_column_values_to_match_regex",
            "kwargs": {"column": "status", "regex": "[a-z]+"},
        },
    ]
}


@pytest.fixture()
def orders_df(spark):
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    rows = [
        ("o-1", "created", 10.0),
        ("o-2", "shipped", 20.0),
        ("o-3", None, 30.0),  # null status: excluded from set/regex, fine elsewhere
        (None, "created", 40.0),  # null pk: not_null violation
        ("o-5", "teleported", -5.0),  # set + between violation
    ]
    schema = StructType(
        [
            StructField("order_id", StringType()),
            StructField("status", StringType()),
            StructField("amount", DoubleType()),
        ]
    )
    return spark.createDataFrame(rows, schema)


@pytest.mark.usefixtures("spark")
class TestSinglePassContract:
    def test_whole_suite_is_one_aggregate_job(self, spark, monkeypatch):
        """The headline guarantee: N expectations compile to exactly ONE agg."""
        from pyspark.sql import DataFrame

        calls = {"n": 0}
        original_agg = DataFrame.agg

        def counting_agg(self, *exprs):
            calls["n"] += 1
            return original_agg(self, *exprs)

        monkeypatch.setattr(DataFrame, "agg", counting_agg)

        df = spark.createDataFrame([("a", "x", 1.0)], ["order_id", "status", "amount"])
        results = DistributedExpectationEngine().evaluate(df, SUITE)
        assert len(results) == len(SUITE["expectations"])
        assert calls["n"] == 1, f"engine ran {calls['n']} aggregate passes; must be 1"


@pytest.mark.usefixtures("spark")
class TestExpectationSemantics:
    def test_full_suite_counts_and_scores(self, orders_df):
        results = {
            r.expectation_type: r for r in DistributedExpectationEngine().evaluate(orders_df, SUITE)
        }

        nn = results["expect_column_values_to_not_be_null"]
        assert not nn.success and nn.unexpected_count == 1 and nn.element_count == 5

        uniq = results["expect_column_values_to_be_unique"]
        assert uniq.success and uniq.observed_value == 1.0

        in_set = results["expect_column_values_to_be_in_set"]
        # null status EXCLUDED (GE semantics): denom=4, violations=1 ('teleported')
        assert in_set.element_count == 4
        assert in_set.unexpected_count == 1
        assert not in_set.success  # 3/4 = 75% < default mostly 100%

        between = results["expect_column_values_to_be_between"]
        # every row has non-null amount → denom=5; only -5.0 violates range
        assert between.element_count == 5
        assert between.unexpected_count == 1
        assert between.observed_value == {"min": -5.0, "max": 40.0}

        rowcount = results["expect_table_row_count_to_be_between"]
        assert rowcount.success and rowcount.observed_value == 5

        mean = results["expect_column_mean_to_be_between"]
        assert mean.success and abs(mean.observed_value - 19.0) < 1e-9  # (10+20+30+40-5)/5

        regex = results["expect_column_values_to_match_regex"]
        # start-anchored [a-z]+ : 'teleported' ok, others lowercase ok → all non-null pass
        assert regex.success

        ordered = results["expect_table_columns_to_match_ordered_list"]
        assert ordered.success

    def test_nulls_do_not_fail_set_expectation(self, orders_df):
        suite = {
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_be_in_set",
                    "kwargs": {"column": "status", "value_set": ["created", "shipped"]},
                },
            ]
        }
        result = DistributedExpectationEngine().evaluate(orders_df, suite)[0]
        assert result.element_count == 4  # o-3's null excluded
        assert result.unexpected_count == 1  # 'teleported'

    def test_mostly_threshold(self, orders_df):
        suite = {
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_be_between",
                    "kwargs": {
                        "column": "amount",
                        "min_value": 0,
                        "max_value": 1000,
                        "mostly": 0.75,
                    },
                },  # 3/4 valid → passes at mostly=0.75
            ]
        }
        result = DistributedExpectationEngine().evaluate(orders_df, suite)[0]
        assert result.success

    def test_unique_detects_duplicates_without_window(self, spark):
        df = spark.createDataFrame([("k",), ("k",), ("k",), ("z",)], ["id"])
        suite = {
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": "id"},
                },
            ]
        }
        result = DistributedExpectationEngine().evaluate(df, suite)[0]
        assert result.unexpected_count == 2  # 2 extra occurrences of 'k'
        assert not result.success

    def test_regex_start_anchoring_matches_ge(self, spark):
        df = spark.createDataFrame([("PAY-1",), ("pay-2",)], ["code"])
        suite = {
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_match_regex",
                    "kwargs": {"column": "code", "regex": "pay"},
                },  # unanchored → ^pay
            ]
        }
        result = DistributedExpectationEngine().evaluate(df, suite)[0]
        assert result.element_count == 2
        assert result.unexpected_count == 1  # 'PAY-1' fails start-anchored match

    def test_unsupported_expectation_fails_not_crashes(self, orders_df):
        suite = {
            "expectations": [
                {
                    "expectation_type": "expect_column_pair_values_a_to_be_greater_than_b",
                    "kwargs": {"col_A": "x", "col_B": "y"},
                },
            ]
        }
        result = DistributedExpectationEngine().evaluate(orders_df, suite)[0]
        assert not result.success
        assert "unsupported" in (result.details or {}).get("evaluation_error", "")


@pytest.mark.usefixtures("spark")
class TestParityWithPandasEngine:
    def test_both_engines_agree_on_score(self, orders_df, tmp_path):
        """Same data + same suite through pandas driver path and Spark path."""
        import json

        import pandas as pd

        from src.validation.data_quality import (
            DataLayer,
            DataQualityConfig,
            DataQualityFramework,
            SuiteConfig,
        )

        suites_dir = tmp_path / "suites"
        suites_dir.mkdir()
        (suites_dir / "parity_suite.json").write_text(json.dumps(SUITE))

        framework = DataQualityFramework(
            DataQualityConfig(
                suites_dir=str(suites_dir),
                reports_output_dir=str(tmp_path / "reports"),
                raise_on_failure=False,
            )
        )
        cfg = SuiteConfig(suite_name="parity_suite", layer=DataLayer.SILVER, dataset_name="orders")

        pdf = pd.DataFrame(
            [(r["order_id"], r["status"], r["amount"]) for r in orders_df.collect()],
            columns=["order_id", "status", "amount"],
        )
        pandas_result = framework.validate(pdf.copy(), cfg)
        spark_result = framework.validate_spark_distributed(orders_df, cfg)

        assert pandas_result.data_quality_score == pytest.approx(spark_result.data_quality_score)
        assert pandas_result.passed_expectations == spark_result.passed_expectations


@pytest.mark.usefixtures("spark")
class TestViolationExtractor:
    def test_returns_only_failing_rows_with_reasons(self, orders_df):
        vd = ViolationExtractor().violations(orders_df, SUITE).collect()
        ids = {r["order_id"] for r in vd}
        assert ids == {None, "o-5"}
        bad_row = next(r for r in vd if r["order_id"] == "o-5")
        reasons = set(bad_row["violated_expectations"])
        assert any("in_set" in x for x in reasons)
        assert any("between" in x for x in reasons)


def test_anchor_pattern():
    assert anchor_pattern("[a-z]+") == "^[a-z]+"
    assert anchor_pattern("^already") == "^already"
