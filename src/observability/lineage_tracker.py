"""
lineage_tracker.py
------------------
OpenLineage event emission — column-level-ready job/run lineage without the
openlineage-python dependency.

Emits spec-compliant OpenLineage v1 JSON (`run/start`, `run/complete`,
`run/fail`) to a Marquez-compatible endpoint:

    POST {url}/api/v1/lineage

Event shape:
    { eventType, eventTime, producer,
      job:   { namespace, name },
      run:   { runId },
      inputs:  [ {namespace, name, facets} ],
      outputs: [ {namespace, name, facets} ] }

Design rules:
  - Lineage must NEVER break the pipeline it describes: every failure mode
    (endpoint down, bad payload, disabled) degrades to a log line.
  - runId is caller-supplied so it matches the PipelineMonitor run_id and
    Delta txn context — one identifier across observability systems.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

PRODUCER = "https://github.com/sheevec/enterprise-data-platform"


@dataclass(frozen=True)
class LineageDataset:
    """An input or output of a job."""

    name: str  # e.g. table path gs://bucket/silver/payments
    namespace: str = "edp-gcs"  # e.g. edp-gcs | edp-bigquery | kafka:<cluster>
    facets: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"namespace": self.namespace, "name": self.name}
        if self.facets:
            out["facets"] = self.facets
        return out


class LineageEmitter:
    """Thin OpenLineage HTTP client; safe-by-default (never raises)."""

    def __init__(
        self,
        enabled: Optional[bool] = None,
        url: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> None:
        self.enabled = (
            enabled
            if enabled is not None
            else os.getenv("LINEAGE_ENABLED", "false").lower() == "true"
        )
        resolved_url = url or os.getenv("LINEAGE_URL") or ""
        self.url = resolved_url.rstrip("/")
        self.namespace = namespace or os.getenv("LINEAGE_NAMESPACE", "enterprise-data-platform")
        self._session = requests.Session()

    # ------------------------------------------------------------------
    def emit_start(
        self,
        job_name: str,
        run_id: str,
        inputs: List[LineageDataset],
        outputs: List[LineageDataset],
    ) -> None:
        self._emit(job_name, run_id, "START", inputs, outputs)

    def emit_complete(
        self,
        job_name: str,
        run_id: str,
        inputs: List[LineageDataset],
        outputs: List[LineageDataset],
    ) -> None:
        self._emit(job_name, run_id, "COMPLETE", inputs, outputs)

    def emit_fail(
        self,
        job_name: str,
        run_id: str,
        inputs: List[LineageDataset],
        outputs: List[LineageDataset],
        error_message: str = "",
    ) -> None:
        self._emit(job_name, run_id, "FAIL", inputs, outputs, error_message=error_message)

    # ------------------------------------------------------------------
    def _emit(
        self,
        job_name: str,
        run_id: str,
        event_type: str,
        inputs: List[LineageDataset],
        outputs: List[LineageDataset],
        error_message: str = "",
    ) -> None:
        if not self.enabled:
            return
        if not self.url:
            logger.debug("LINEAGE_URL unset — dropping %s event for %s", event_type, job_name)
            return

        from datetime import datetime, timezone

        event: Dict[str, Any] = {
            "eventType": event_type,
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "producer": PRODUCER,
            "job": {"namespace": self.namespace, "name": job_name},
            "run": {"runId": run_id},
            "inputs": [d.to_dict() for d in inputs],
            "outputs": [d.to_dict() for d in outputs],
        }
        if error_message:
            event["runFacets"] = {
                "errorMessage": {
                    "_producer": PRODUCER,
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ErrorFacet.json",
                    "message": error_message[:2000],
                    "programmingLanguage": "python",
                }
            }

        try:
            response = self._session.post(
                f"{self.url}/api/v1/lineage",
                data=json.dumps(event),
                headers={"Content-Type": "application/json"},
                timeout=5,
            )
            response.raise_for_status()
            logger.debug("Lineage %s emitted | job=%s", event_type, job_name)
        except Exception as exc:
            logger.warning("Lineage emission failed (non-fatal) | job=%s | error=%s", job_name, exc)


def build_emitter_from_env() -> LineageEmitter:
    return LineageEmitter()


def new_run_id() -> str:
    return str(uuid.uuid4())
