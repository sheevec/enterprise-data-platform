"""
schema_registry.py
------------------
Confluent Schema Registry client shared by all ingestion paths (the custom
kafka consumer and the Spark Structured Streaming Bronze job).

Responsibilities:
  - Fetch and cache Avro schemas by numeric schema ID (Confluent wire format)
  - Resolve the latest schema for a subject (used as the Spark reader schema)
  - List all registered schema IDs for a subject (allow-list for stream decode)
  - Compatibility pre-flight check usable in CI before promoting producers

Confluent wire format: [0x00][4-byte big-endian schema_id][avro_payload]
"""

from __future__ import annotations

import io
import json
import logging
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import fastavro
import requests

logger = logging.getLogger(__name__)


@dataclass
class SchemaRegistryConfig:
    """Connection settings for Confluent Schema Registry."""

    url: str
    basic_auth_user_info: Optional[str] = None  # "key:secret" for Confluent Cloud


class SchemaRegistryClient:
    """
    Thread-safe, caching client for Confluent Schema Registry.

    Used by:
      - DataPlatformConsumer (decode messages in flight)
      - bronze_streaming job (resolve reader schemas + ID allow-lists at startup)
      - CI compatibility checks (assert BACKWARD compatibility before deploy)
    """

    MAGIC_BYTE = 0x00
    SCHEMA_ID_LENGTH = 4

    def __init__(self, config: SchemaRegistryConfig) -> None:
        self._base_url = config.url.rstrip("/")
        self._session = requests.Session()
        if config.basic_auth_user_info:
            key, secret = config.basic_auth_user_info.split(":", 1)
            self._session.auth = (key, secret)
        self._schema_cache: Dict[int, Any] = {}  # schema_id -> parsed avro schema
        self._raw_schema_cache: Dict[int, str] = {}  # schema_id -> raw JSON string
        self._subject_versions_cache: Dict[str, List[int]] = {}
        self._cache_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Schema-by-ID (wire format decoding path)
    # ------------------------------------------------------------------

    def get_raw_schema_json(self, schema_id: int) -> str:
        """Return the raw JSON schema string for a schema ID (cached)."""
        with self._cache_lock:
            if schema_id in self._raw_schema_cache:
                return self._raw_schema_cache[schema_id]

        url = f"{self._base_url}/schemas/ids/{schema_id}"
        response = self._session.get(url, timeout=10)
        response.raise_for_status()
        schema_str = response.json()["schema"]

        with self._cache_lock:
            self._raw_schema_cache[schema_id] = schema_str
        logger.debug("Fetched raw schema id=%d from registry", schema_id)
        return schema_str

    def get_schema(self, schema_id: int) -> Any:
        """Return a parsed fastavro schema for the given schema ID (cached)."""
        with self._cache_lock:
            if schema_id in self._schema_cache:
                return self._schema_cache[schema_id]

        parsed = fastavro.parse_schema(json.loads(self.get_raw_schema_json(schema_id)))
        with self._cache_lock:
            self._schema_cache[schema_id] = parsed
        logger.debug("Fetched and cached schema id=%d from registry", schema_id)
        return parsed

    def decode_message(self, raw_bytes: bytes) -> Dict[str, Any]:
        """
        Decode a Confluent wire-format Avro message.
        Returns the deserialized Python dict.
        """
        if len(raw_bytes) < 5 or raw_bytes[0] != self.MAGIC_BYTE:
            raise ValueError(
                f"Invalid Confluent wire format: magic byte missing or wrong "
                f"(got 0x{raw_bytes[0]:02x} expected 0x00)"
            )

        schema_id = int.from_bytes(raw_bytes[1:5], byteorder="big")
        schema = self.get_schema(schema_id)
        avro_payload = raw_bytes[5:]

        with io.BytesIO(avro_payload) as buf:
            record = fastavro.schemaless_reader(buf, schema)
        return record

    # ------------------------------------------------------------------
    # Subject-level operations (Spark reader schema + CI compat checks)
    # ------------------------------------------------------------------

    def get_latest_schema(self, subject: str) -> tuple[int, str]:
        """
        Return (schema_id, raw_json_schema) for the latest version of subject.
        The raw JSON string plugs directly into Spark's from_avro().
        """
        meta = self._get(f"/subjects/{subject}/versions/latest")
        return int(meta["id"]), meta["schema"]

    def list_subject_schema_ids(self, subject: str) -> List[int]:
        """Return every schema ID ever registered for subject (all versions)."""
        with self._cache_lock:
            if subject in self._subject_versions_cache:
                return self._subject_versions_cache[subject]

        versions = self._get(f"/subjects/{subject}/versions")
        ids = sorted({int(self._get(f"/subjects/{subject}/versions/{v}")["id"]) for v in versions})

        with self._cache_lock:
            self._subject_versions_cache[subject] = ids
        return ids

    def check_backward_compatible(self, subject: str) -> bool:
        """
        Assert that the latest registered schema of `subject` is backward
        compatible with all prior versions. Wire this into CI: producers must
        pass this gate before their deploy proceeds.
        """
        result = self._session.post(
            f"{self._base_url}/compatibility/subjects/{subject}/versions/latest",
            json={"compatibility": "BACKWARD"},
            timeout=10,
        )
        if result.status_code == 404:
            logger.info("Subject %s has no prior versions — nothing to check", subject)
            return True
        result.raise_for_status()
        compatible = bool(result.json().get("is_compatible", False))
        if not compatible:
            logger.error("Schema for subject=%s is NOT backward compatible", subject)
        return compatible

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _get(self, path: str) -> Any:
        response = self._session.get(f"{self._base_url}{path}", timeout=10)
        response.raise_for_status()
        return response.json()
