"""
distributed_dq.py
-----------------
Spark-native expectation engine: validates FULL tables in-cluster.

Why this exists (the driver-sampling trap):
  The pandas engine pulls data to one machine — `sample(10%).toPandas()`
  still means tens of GB on one driver at TB scale, and a 10% sample
  statistically misses rare-but-fatal defects (a 0.1% corruption shows up
  in any given 10% sample only ~9.5% of the time). Distributed validation
  scans everything, where the data already lives.

Design: N expectations → ONE Spark job.
  Every row-level expectation compiles to a boolean violation flag plus a
  denominator condition. All flags/denominators/observed-aggregates merge
  into a single df.agg(...) pass — whether the suite has 5 or 500
  expectations, the table is scanned once. Table-shape checks (column
  existence/order/type) resolve from the schema for free.

Alias contract: handler registered at plan index i owns aggregates named
  v{i} (violation rows), d{i} (evaluated denominator rows), e{i}_* (extras).
The resolver reads aliases from the collected single Row via names carried
in each expectation's context dict.

Null semantics follow Great Expectations:
  - not_null:              nulls ARE unexpected violations
  - between/in_set/regex:  nulls EXCLUDED entirely (no violation, no denom)
  - mean/sum/unique-count: nulls ignored natively
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src.validation.data_quality import ExpectationResult

logger = logging.getLogger(__name__)

_ROW_COUNT_ALIAS = "__total_rows__"


# ---------------------------------------------------------------------------
# Aggregation plan
# ---------------------------------------------------------------------------


class _AggPlan:
    """Collects every aggregate expression needed for the single pass."""

    def __init__(self) -> None:
        self.exprs: List[Column] = []
        self.index = 0

    def next_index(self) -> int:
        idx = self.index
        self.index += 1
        return idx

    def add_violation(self, i: int, flag: Column) -> str:
        alias = f"v{i}"
        self.exprs.append(F.count(F.when(flag, 1)).alias(alias))
        return alias

    def add_denominator(self, i: int, cond: Column) -> str:
        alias = f"d{i}"
        self.exprs.append(F.count(F.when(cond, 1)).alias(alias))
        return alias

    def add_extra(self, i: int, key: str, agg: Column) -> str:
        alias = f"e{i}_{key}"
        self.exprs.append(agg.alias(alias))
        return alias


# ---------------------------------------------------------------------------
# Flag builders: (plan_index, ctx_with_aliases)
# ---------------------------------------------------------------------------


def _flag_not_null(
    df: DataFrame, kwargs: Dict[str, Any], plan: _AggPlan
) -> Tuple[int, Dict[str, Any]]:
    i = plan.next_index()
    col = F.col(kwargs["column"])
    v = plan.add_violation(i, col.isNull())
    d = plan.add_denominator(i, F.lit(True))
    pct = plan.add_extra(i, "nonnull_pct", F.avg(col.isNotNull().cast("double")))
    return i, {"column": kwargs["column"], "v": v, "d": d, "nonnull_pct": pct}


def _flag_between(
    df: DataFrame, kwargs: Dict[str, Any], plan: _AggPlan
) -> Tuple[int, Dict[str, Any]]:
    i = plan.next_index()
    raw = F.col(kwargs["column"])
    numeric = raw.cast("double")
    non_null = raw.isNotNull()
    in_range = F.lit(True)
    if kwargs.get("min_value") is not None:
        in_range = in_range & (numeric >= float(kwargs["min_value"]))
    if kwargs.get("max_value") is not None:
        in_range = in_range & (numeric <= float(kwargs["max_value"]))
    violation = non_null & (~in_range | numeric.isNull())
    v = plan.add_violation(i, violation)
    d = plan.add_denominator(i, non_null)
    mn = plan.add_extra(i, "min", F.min(numeric))
    mx = plan.add_extra(i, "max", F.max(numeric))
    return i, {
        "column": kwargs["column"],
        "v": v,
        "d": d,
        "min": mn,
        "max": mx,
        "min_value": kwargs.get("min_value"),
        "max_value": kwargs.get("max_value"),
        "mostly": kwargs.get("mostly", 1.0),
    }


def _flag_in_set(
    df: DataFrame, kwargs: Dict[str, Any], plan: _AggPlan
) -> Tuple[int, Dict[str, Any]]:
    i = plan.next_index()
    col = F.col(kwargs["column"])
    value_set = list(kwargs["value_set"])
    non_null = col.isNotNull()
    v = plan.add_violation(i, non_null & ~col.isin(value_set))
    d = plan.add_denominator(i, non_null)
    return i, {
        "column": kwargs["column"],
        "v": v,
        "d": d,
        "value_set": value_set,
        "mostly": kwargs.get("mostly", 1.0),
    }


def _flag_regex(
    df: DataFrame, kwargs: Dict[str, Any], plan: _AggPlan
) -> Tuple[int, Dict[str, Any]]:
    i = plan.next_index()
    col = F.col(kwargs["column"])
    pattern = anchor_pattern(kwargs["regex"])  # GE re.match semantics
    matched = col.rlike(pattern)
    non_null = col.isNotNull()
    v = plan.add_violation(i, non_null & ~matched)
    d = plan.add_denominator(i, non_null)
    rate = plan.add_extra(i, "match_rate", F.avg(matched.cast("double")))
    return i, {
        "column": kwargs["column"],
        "v": v,
        "d": d,
        "pattern": pattern,
        "match_rate": rate,
        "mostly": kwargs.get("mostly", 1.0),
    }


def _flag_unique(
    df: DataFrame, kwargs: Dict[str, Any], plan: _AggPlan
) -> Tuple[int, Dict[str, Any]]:
    """
    Extra duplicates beyond first occurrence per value — computed WITHOUT a
    shuffling window: duplicates = count(non-null) - countDistinct(non-null).
    Matches pandas Series.duplicated().sum() semantics exactly.
    """
    i = plan.next_index()
    col = F.col(kwargs["column"])
    nn = plan.add_extra(i, "nonnull_count", F.count(col))
    nd = plan.add_extra(i, "distinct_count", F.countDistinct(col))
    return i, {"column": kwargs["column"], "nn": nn, "nd": nd, "mostly": kwargs.get("mostly", 1.0)}


def _agg_mean(df: DataFrame, kwargs: Dict[str, Any], plan: _AggPlan) -> Tuple[int, Dict[str, Any]]:
    i = plan.next_index()
    m = plan.add_extra(i, "mean", F.avg(F.col(kwargs["column"]).cast("double")))
    return i, {
        "column": kwargs["column"],
        "mean": m,
        "min_value": kwargs.get("min_value"),
        "max_value": kwargs.get("max_value"),
    }


def _agg_sum(df: DataFrame, kwargs: Dict[str, Any], plan: _AggPlan) -> Tuple[int, Dict[str, Any]]:
    i = plan.next_index()
    s = plan.add_extra(i, "sum", F.sum(F.col(kwargs["column"])))
    return i, {
        "column": kwargs["column"],
        "sum": s,
        "min_value": kwargs.get("min_value"),
        "max_value": kwargs.get("max_value"),
    }


_DYNAMIC_HANDLERS = {
    "expect_column_values_to_not_be_null": _flag_not_null,
    "expect_column_values_to_be_between": _flag_between,
    "expect_column_values_to_be_in_set": _flag_in_set,
    "expect_column_values_to_match_regex": _flag_regex,
    "expect_column_values_to_be_unique": _flag_unique,
    "expect_column_mean_to_be_between": _agg_mean,
    "expect_column_sum_to_be_between": _agg_sum,
}

_STATIC_HANDLERS = {
    "expect_column_to_exist",
    "expect_table_columns_to_match_ordered_list",
    "expect_column_values_to_be_of_type",
}

_ROW_COUNT_TYPE = "expect_table_row_count_to_be_between"


# ---------------------------------------------------------------------------
# Static / row-count result builders
# ---------------------------------------------------------------------------


def _static_result(
    exp_type: str, kwargs: Dict[str, Any], columns: List[str], dtypes: Dict[str, str]
) -> ExpectationResult:
    if exp_type == "expect_column_to_exist":
        ok = kwargs["column"] in columns
        return ExpectationResult(
            exp_type, kwargs["column"], ok, list(columns), len(columns), 0 if ok else 1, 0.0
        )

    if exp_type == "expect_table_columns_to_match_ordered_list":
        expected = kwargs["column_list"]
        ok = list(columns) == expected
        return ExpectationResult(
            exp_type,
            None,
            ok,
            list(columns),
            len(columns),
            0 if ok else 1,
            0.0,
            details={"expected": expected},
        )

    if exp_type == "expect_column_values_to_be_of_type":
        want = str(kwargs["type_"]).lower()
        got = dtypes.get(kwargs["column"], "<missing>")
        ok = want in got.lower()
        return ExpectationResult(
            exp_type,
            kwargs["column"],
            ok,
            got,
            0,
            0 if ok else 1,
            0.0,
            details={"expected_type": want},
        )

    raise NotImplementedError(exp_type)


def _row_count_result(row_count: int, kwargs: Dict[str, Any]) -> ExpectationResult:
    min_val = kwargs.get("min_value", 0)
    max_val = kwargs.get("max_value")
    in_range = row_count >= min_val and (max_val is None or row_count <= max_val)
    return ExpectationResult(
        _ROW_COUNT_TYPE,
        None,
        in_range,
        row_count,
        row_count,
        0 if in_range else 1,
        0.0 if in_range else 100.0,
    )


def _range_ok(value: Optional[float], min_value: Any, max_value: Any) -> bool:
    if value is None:
        return False
    if min_value is not None and value < float(min_value):
        return False
    if max_value is not None and value > float(max_value):
        return False
    return True


def anchor_pattern(pattern: str) -> str:
    return pattern if pattern.startswith("^") else f"^{pattern}"


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class DistributedExpectationEngine:
    """Evaluate an entire expectation suite against a Spark DF in ONE pass."""

    def evaluate(self, df: DataFrame, suite: Dict[str, Any]) -> List[ExpectationResult]:
        expectations: List[Dict[str, Any]] = suite.get("expectations", [])
        if not expectations:
            logger.warning("Suite contains no expectations.")
            return []

        columns = list(df.columns)
        dtypes: Dict[str, str] = dict(df.dtypes)

        resolved: Dict[int, ExpectationResult] = {}
        pending: List[Tuple[int, str, Dict[str, Any]]] = []  # (order, type, ctx)
        row_count_ctx: Optional[Tuple[int, Dict[str, Any]]] = None

        plan = _AggPlan()

        for order, expectation in enumerate(expectations):
            exp_type = expectation.get("expectation_type", "")
            kwargs = expectation.get("kwargs", {})
            try:
                if exp_type in _STATIC_HANDLERS:
                    resolved[order] = _static_result(exp_type, kwargs, columns, dtypes)
                elif exp_type == _ROW_COUNT_TYPE:
                    row_count_ctx = (order, dict(kwargs))
                    pending.append((order, exp_type, kwargs))
                elif exp_type in _DYNAMIC_HANDLERS:
                    _, ctx = _DYNAMIC_HANDLERS[exp_type](df, kwargs, plan)
                    pending.append((order, exp_type, ctx))
                else:
                    resolved[order] = self._unsupported(exp_type, kwargs)
            except Exception as exc:
                logger.warning("Expectation compile error | type=%s | error=%s", exp_type, exc)
                resolved[order] = self._errored(exp_type, kwargs, str(exc))

        # ---------- THE single pass over data ----------
        agg_exprs = [*plan.exprs, F.count(F.lit(1)).alias(_ROW_COUNT_ALIAS)]
        row = df.agg(*agg_exprs).collect()[0]
        total_rows = int(row[_ROW_COUNT_ALIAS])

        if row_count_ctx is not None:
            order, kwargs = row_count_ctx
            resolved[order] = _row_count_result(total_rows, kwargs)

        for order, exp_type, ctx in pending:
            if exp_type == _ROW_COUNT_TYPE:
                continue
            try:
                resolved[order] = self._resolve(exp_type, ctx, row, total_rows)
            except Exception as exc:
                logger.warning("Expectation resolve error | type=%s | error=%s", exp_type, exc)
                resolved[order] = self._errored(exp_type, ctx, str(exc))

        return [resolved[i] for i in sorted(resolved)]

    # ------------------------------------------------------------------
    def _resolve(
        self, exp_type: str, ctx: Dict[str, Any], row: Any, total_rows: int
    ) -> ExpectationResult:
        def num(alias: str) -> int:
            v = row[alias]
            return int(v) if v is not None else 0

        mostly = float(ctx.get("mostly", 1.0))

        if exp_type == "expect_column_mean_to_be_between":
            observed = row[ctx["mean"]]
            ok = _range_ok(
                float(observed) if observed is not None else None,
                ctx["min_value"],
                ctx["max_value"],
            )
            return ExpectationResult(
                exp_type, ctx["column"], ok, observed, total_rows, 0 if ok else 1, 0.0
            )

        if exp_type == "expect_column_sum_to_be_between":
            observed = row[ctx["sum"]]
            ok = _range_ok(
                float(observed) if observed is not None else None,
                ctx["min_value"],
                ctx["max_value"],
            )
            return ExpectationResult(
                exp_type, ctx["column"], ok, observed, total_rows, 0 if ok else 1, 0.0
            )

        if exp_type == "expect_column_values_to_be_unique":
            dup = max(0, num(ctx["nn"]) - num(ctx["nd"]))
            unique_pct = (total_rows - dup) / total_rows if total_rows else 1.0
            return ExpectationResult(
                exp_type,
                ctx["column"],
                unique_pct >= mostly,
                unique_pct,
                total_rows,
                dup,
                (dup / total_rows * 100) if total_rows else 0.0,
            )

        # v/d alias expectations: not_null, between, in_set, regex
        denom = num(ctx["d"])
        violations = num(ctx["v"])
        valid_pct = ((denom - violations) / denom) if denom else 1.0

        if exp_type == "expect_column_values_to_not_be_null":
            observed = (
                float(row[ctx["nonnull_pct"]]) if row[ctx["nonnull_pct"]] is not None else 1.0
            )
        elif exp_type == "expect_column_values_to_be_between":
            observed = {"min": row[ctx["min"]], "max": row[ctx["max"]]}
        elif exp_type == "expect_column_values_to_match_regex":
            rate = row[ctx["match_rate"]]
            observed = {
                "regex": ctx["pattern"],
                "match_rate": float(rate) if rate is not None else 1.0,
            }
        elif exp_type == "expect_column_values_to_be_in_set":
            observed = ctx["value_set"]
        else:  # pragma: no cover — guarded by dispatch table
            raise NotImplementedError(exp_type)

        return ExpectationResult(
            expectation_type=exp_type,
            column=ctx.get("column"),
            success=(valid_pct >= mostly) if denom else True,
            observed_value=observed,
            element_count=denom,
            unexpected_count=violations,
            unexpected_percent=((violations / denom) * 100) if denom else 0.0,
        )

    @staticmethod
    def _unsupported(exp_type: str, kwargs: Dict[str, Any]) -> ExpectationResult:
        return ExpectationResult(
            expectation_type=exp_type,
            column=kwargs.get("column"),
            success=False,
            observed_value=None,
            element_count=0,
            unexpected_count=None,
            unexpected_percent=None,
            details={"evaluation_error": f"unsupported in distributed engine: {exp_type}"},
        )

    @staticmethod
    def _errored(exp_type: str, kwargs: Dict[str, Any], message: str) -> ExpectationResult:
        return ExpectationResult(
            expectation_type=exp_type,
            column=kwargs.get("column") if isinstance(kwargs, dict) else None,
            success=False,
            observed_value=None,
            element_count=0,
            unexpected_count=None,
            unexpected_percent=None,
            details={"evaluation_error": message},
        )


# ---------------------------------------------------------------------------
# Violation extraction (lazy — caller decides how many offenders to surface)
# ---------------------------------------------------------------------------


class ViolationExtractor:
    """
    Rebuilds violation flags for a suite and returns the failing ROWS as a
    lazy Spark DataFrame with a `violated_expectations` array column.
    Costs one more scan than evaluate(); call only when failures exist and
    you need examples (e.g., top-N offenders into the incident payload).
    """

    def violations(self, df: DataFrame, suite: Dict[str, Any]) -> DataFrame:
        flags: List[Tuple[str, Column]] = []
        for expectation in suite.get("expectations", []):
            exp_type = expectation.get("expectation_type", "")
            kwargs = expectation.get("kwargs", {})

            if exp_type == "expect_column_values_to_not_be_null":
                flags.append((exp_type + ":" + kwargs["column"], F.col(kwargs["column"]).isNull()))
            elif exp_type == "expect_column_values_to_be_in_set":
                col = F.col(kwargs["column"])
                flags.append(
                    (
                        exp_type + ":" + kwargs["column"],
                        col.isNotNull() & ~col.isin(list(kwargs["value_set"])),
                    )
                )
            elif exp_type == "expect_column_values_to_match_regex":
                col = F.col(kwargs["column"])
                anchored = anchor_pattern(kwargs["regex"])
                flags.append(
                    (exp_type + ":" + kwargs["column"], col.isNotNull() & ~col.rlike(anchored))
                )
            elif exp_type == "expect_column_values_to_be_between":
                raw = F.col(kwargs["column"])
                numeric = raw.cast("double")
                bad = raw.isNotNull() & numeric.isNull()
                if kwargs.get("min_value") is not None:
                    bad = bad | (numeric < float(kwargs["min_value"]))
                if kwargs.get("max_value") is not None:
                    bad = bad | (numeric > float(kwargs["max_value"]))
                flags.append((exp_type + ":" + kwargs["column"], bad))

        if not flags:
            return df.where(F.lit(False)).limit(0)

        agg_col = F.array(*[F.when(cond, F.lit(name)) for name, cond in flags])
        violated = F.filter(agg_col, lambda x: x.isNotNull())
        return df.withColumn("violated_expectations", violated).where(F.size(violated) > 0)
