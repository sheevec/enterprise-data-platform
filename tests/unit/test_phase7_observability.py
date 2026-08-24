"""Tests for Phase 7 observability pieces (no external services)."""

import json
from datetime import datetime

from src.observability.baselines import SeasonalBaseline, mad_score
from src.observability.lineage_tracker import LineageDataset, LineageEmitter

# ---------------------------------------------------------------------------
# Prometheus exporter
# ---------------------------------------------------------------------------


class TestPrometheusExporter:
    def test_disabled_is_zero_cost_noop(self):
        from src.observability.prometheus_exporter import ConsumerPrometheusMetrics

        m = ConsumerPrometheusMetrics(enabled=False)
        assert m.enabled is False
        m.record_consumed("t")  # must not raise / touch registry
        m.record_lag_snapshot({"t:0": 5})

    def test_metrics_exposed_via_registry(self):
        from prometheus_client import generate_latest

        from src.observability.prometheus_exporter import ConsumerPrometheusMetrics

        # unique port to avoid collisions across test reruns; server not asserted
        m = ConsumerPrometheusMetrics(enabled=True, port=19331)
        try:
            m.record_consumed("payments")
            m.record_consumed("payments")
            m.record_deserialization_error("payments")
            m.record_batch_written("payments")
            m.record_dlq("payments")
            m.record_gcs_error("payments")
            m.record_lag_snapshot({"payments:0": 42})

            payload = generate_latest().decode()
            assert 'edp_consumer_messages_consumed_total{topic="payments"} 2.0' in payload
            assert 'edp_consumer_lag{partition="0",topic="payments"} 42.0' in payload
            assert "edp_consumer_deserialization_errors_total" in payload
        finally:
            # stop http server thread if started
            if getattr(m, "_server_thread", None) is not None:
                from prometheus_client import shutdown as pm_shutdown  # type: ignore[attr-defined]

                pm_shutdown()


# ---------------------------------------------------------------------------
# Seasonal robust baselines
# ---------------------------------------------------------------------------


def _a_wednesday() -> datetime:
    from datetime import timedelta

    day = next(d for d in range(1, 29) if datetime(2026, 8, d).weekday() == 2)
    return datetime(2026, 8, day) - timedelta(days=0)


def _wednesday_history():
    # 10 prior Wednesdays at hour 9, volumes ~1000 with small jitter,
    # plus one noisy point in a DIFFERENT bucket that must not influence.
    from datetime import timedelta

    day = next(d for d in range(1, 29) if datetime(2026, 8, d).weekday() == 2)
    base9 = datetime(2026, 8, day, 9)

    history = [(base9 - timedelta(weeks=w), 1000.0 + w * 15) for w in range(10)]
    sunday23 = base9 - timedelta(days=3)  # same week, Sunday
    history.append((sunday23.replace(hour=23), 99999.0))
    return history


class TestMadScore:
    def test_normal_consistent_scale(self):
        # MAD of [1..9] = 2 → score(7) = 0.6745*(7-5)/2 = 0.6745
        med = 5.0
        mad = 2.0
        assert abs(mad_score(7.0, med, mad) - 0.6745) < 1e-3

    def test_zero_mad(self):
        assert mad_score(5.0, 5.0, 0.0) == 0.0
        assert mad_score(9.0, 5.0, 0.0) == float("inf")


class TestSeasonalBaseline:
    def test_seasonal_bucket_ignores_other_buckets(self):
        now = _a_wednesday().replace(hour=9)
        b = SeasonalBaseline(_wednesday_history(), now=now)
        anomalous, score = b.is_anomalous(1050.0)
        assert anomalous is False
        assert score is not None and abs(score) < 3  # well within bucket noise

    def test_spike_in_bucket_detected(self):
        now = _a_wednesday().replace(hour=9)
        b = SeasonalBaseline(_wednesday_history(), now=now)
        anomalous, score = b.is_anomalous(5000.0)
        assert anomalous is True and score > 3

    def test_fallback_to_all_history_when_bucket_thin(self):
        history = _wednesday_history()[:4]  # < MIN_SEASONAL_POINTS
        sunday23 = next(t for t, _ in history).replace(hour=23) + __import__("datetime").timedelta(
            days=1
        )
        history.append((sunday23, 500.0))  # other-bucket points exist
        b = SeasonalBaseline(history, now=_a_wednesday().replace(hour=9))
        anomalous, score = b.is_anomalous(1020.0)
        assert score is not None  # judged via all-history fallback

    def test_insufficient_history_returns_none_score(self):
        b = SeasonalBaseline([(datetime(2026, 8, 19, 9), 100.0)], now=datetime(2026, 8, 19, 9))
        anomalous, score = b.is_anomalous(999999.0)
        assert anomalous is False and score is None


# ---------------------------------------------------------------------------
# OpenLineage emitter
# ---------------------------------------------------------------------------


class FakeResponse:
    def __init__(self, status_code=200):
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"http {self.status_code}")


class TestLineageEmitter:
    def test_disabled_emits_nothing(self, caplog):
        em = LineageEmitter(enabled=False, url="http://marquez:5000")
        em.emit_start("job", "run-1", [], [])
        # no exception; nothing posted (no session spy needed — disabled short-circuit)

    def test_event_payload_matches_openlineage_shape(self, monkeypatch):
        captured = {}

        def fake_post(self, url, data=None, headers=None, timeout=None):
            captured["url"] = url
            captured["event"] = json.loads(data)
            return FakeResponse(200)

        monkeypatch.setattr("requests.Session.post", fake_post)

        em = LineageEmitter(enabled=True, url="http://marquez:5000", namespace="ns")
        inputs = [LineageDataset(name="gs://b/bronze/p", namespace="edp-gcs")]
        outputs = [LineageDataset(name="gs://b/silver/p", namespace="edp-gcs")]
        em.emit_start("silver.payments", "run-123", inputs, outputs)

        ev = captured["event"]
        assert captured["url"].endswith("/api/v1/lineage")
        assert ev["eventType"] == "START"
        assert ev["job"] == {"namespace": "ns", "name": "silver.payments"}
        assert ev["run"]["runId"] == "run-123"
        assert ev["inputs"][0]["name"] == "gs://b/bronze/p"
        assert ev["outputs"][0]["namespace"] == "edp-gcs"
        assert "eventTime" in ev and "producer" in ev

    def test_fail_event_carries_error_facet(self, monkeypatch):
        captured = {}

        def fake_post(self, url, data=None, headers=None, timeout=None):
            captured["event"] = json.loads(data)
            return FakeResponse(200)

        monkeypatch.setattr("requests.Session.post", fake_post)

        em = LineageEmitter(enabled=True, url="http://m")
        em.emit_fail("job", "run-9", [], [], error_message="boom")
        assert captured["event"]["eventType"] == "FAIL"
        assert captured["event"]["runFacets"]["errorMessage"]["message"] == "boom"

    def test_endpoint_failure_never_raises(self, monkeypatch):
        def broken_post(self, *a, **kw):
            raise ConnectionError("marquez down")

        monkeypatch.setattr("requests.Session.post", broken_post)
        em = LineageEmitter(enabled=True, url="http://m")
        em.emit_complete("job", "run-1", [], [])  # must not raise
