"""
baselines.py
------------
Seasonality-aware anomaly baselines for pipeline volume/duration.

Why not plain z-score: nightly batch volumes follow daily/weekly cycles. A
global mean/stdev over the last N runs flags every Monday-morning spike and
misses slow decay inside a bucket. Two fixes applied here:

  1. SEASONAL BUCKETING — compare a run only against runs from the same
     season bucket (day-of-week + hour by default).
  2. ROBUST STATISTICS — median + MAD instead of mean/stdev. A single prior
     incident inflates stdev enough to hide the next one; MAD barely moves.

Anomaly score: 0.6745 * (value − median) / MAD  (normal-consistent z-scale,
so existing thresholds like 3.0/5.0 keep their meaning).

Fallback ladder: seasonal bucket needs >= MIN_SEASONAL_POINTS samples,
else all-history robust baseline, else None (not enough data to judge).
"""

from __future__ import annotations

import logging
import statistics
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

MIN_SEASONAL_POINTS = 8
_MIN_TOTAL_POINTS = 4


def mad_score(value: float, median: float, mad: float) -> Optional[float]:
    """Normal-consistent robust z-score. inf when MAD==0 and value differs."""
    if mad == 0:
        return 0.0 if value == median else float("inf")
    return 0.6745 * (value - median) / mad


@dataclass(frozen=True)
class SeasonalBaselineConfig:
    seasonality: str = "day_of_week_hour"  # day_of_week_hour | hour_of_day | none
    max_mad_score: float = 3.0


class SeasonalBaseline:
    """Judge one observation against seasonally-matched robust history."""

    def __init__(
        self,
        history: List[Tuple[datetime, float]],
        now: Optional[datetime] = None,
        config: Optional[SeasonalBaselineConfig] = None,
    ) -> None:
        self._config = config or SeasonalBaselineConfig()
        self._now = now or datetime.now()
        cleaned = [(ts, v) for ts, v in history if v is not None]
        self._seasonal = self._bucket(cleaned)
        self._all_values = [v for _, v in cleaned]

    # ------------------------------------------------------------------
    def _bucket_key(self, ts: datetime) -> Tuple:
        cfg = self._config.seasonality
        if cfg == "day_of_week_hour":
            return (ts.weekday(), ts.hour)
        if cfg == "hour_of_day":
            return (ts.hour,)
        return ()  # 'none': everything shares one bucket

    def _bucket(self, history: List[Tuple[datetime, float]]) -> List[float]:
        key = self._bucket_key(self._now)
        return [v for ts, v in history if self._bucket_key(ts) == key]

    @staticmethod
    def _robust_center(values: List[float]) -> Tuple[Optional[float], Optional[float]]:
        """(median, MAD). None when insufficient data."""
        if len(values) < _MIN_TOTAL_POINTS:
            return None, None
        med = statistics.median(values)
        mad = statistics.median([abs(v - med) for v in values])
        return med, mad

    # ------------------------------------------------------------------
    def is_anomalous(self, value: float) -> Tuple[bool, Optional[float]]:
        """
        Returns (is_anomalous, mad_score_or_None). Score None = insufficient
        history to judge (callers should treat as healthy but log).
        """
        candidates = (
            self._seasonal if len(self._seasonal) >= MIN_SEASONAL_POINTS else self._all_values
        )
        med, mad = self._robust_center(candidates)
        if med is None or mad is None:
            logger.debug(
                "Baseline insufficient | seasonal_points=%d | total=%d",
                len(self._seasonal),
                len(self._all_values),
            )
            return False, None

        score = mad_score(value, med, mad)
        if score is None:
            return False, None
        return abs(score) > self._config.max_mad_score, score
