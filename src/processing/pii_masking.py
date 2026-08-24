"""
pii_masking.py
--------------
Policy-as-code PII protection applied at the Silver boundary (and anywhere
else raw identifiers must not travel).

Strategies:
  tokenize   — HMAC-SHA256(key, value), hex-truncated. DETERMINISTIC: the same
               input always yields the same token, so joins/group-bys keep
               working across masked datasets while the plaintext never
               appears downstream. Reversible ONLY with the key (crypto-
               shredding compatible: destroy the key mapping to erase).
  mask_full  — every character replaced, length preserved ("4111" -> "XXXX")
  mask_partial — keep last 4 chars only ("4111111111111111" -> "XXXX1111");
                 standard for card/phone display formats
  drop       — column removed entirely

The key comes from PII_TOKENIZATION_KEY env (injected via Vault/Secret Manager
in production — NEVER committed). Missing key fails fast when any tokenize
rule is active; masking strategies that need no key still work without one.

Rules are plain data (see PiiRule) so they can live in versioned JSON/YAML and
be reviewed like code — governance as pull request.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

KEY_ENV_VAR = "PII_TOKENIZATION_KEY"
TOKEN_LENGTH_CHARS = 32  # of sha256-hex (64); half is plenty for join keys


class MaskStrategy(str, Enum):
    TOKENIZE = "tokenize"
    MASK_FULL = "mask_full"
    MASK_PARTIAL_LAST4 = "mask_partial_last4"
    DROP = "drop"


@dataclass(frozen=True)
class PiiRule:
    """One column's protection policy."""

    column: str
    strategy: MaskStrategy


def parse_rules(raw: List[Dict[str, str]]) -> List[PiiRule]:
    rules: List[PiiRule] = []
    for item in raw:
        missing = {"column", "strategy"} - set(item)
        if missing:
            raise ValueError(f"PiiRule {item} missing keys: {missing}")
        try:
            strategy = MaskStrategy(item["strategy"])
        except ValueError as exc:
            raise ValueError(
                f"Unknown PII strategy {item['strategy']!r}; "
                f"valid: {[s.value for s in MaskStrategy]}"
            ) from exc
        rules.append(PiiRule(column=str(item["column"]), strategy=strategy))
    return rules


# ---------------------------------------------------------------------------
# Tokenization core (shared by pandas & spark paths via UDF-free spark exprs)
# ---------------------------------------------------------------------------


def _require_key() -> str:
    key = os.getenv(KEY_ENV_VAR)
    if not key:
        raise ValueError(
            f"{KEY_ENV_VAR} is required for tokenize strategy. Inject from "
            "Vault/Secret Manager; never hardcode."
        )
    return key


def tokenize_value(value: str, key: str) -> str:
    digest = hmac.new(key.encode(), str(value).encode(), hashlib.sha256).hexdigest()
    return digest[:TOKEN_LENGTH_CHARS]


# ---------------------------------------------------------------------------
# Pandas path
# ---------------------------------------------------------------------------


def mask_pandas(df: pd.DataFrame, rules: List[PiiRule], key: Optional[str] = None) -> pd.DataFrame:
    out = df.copy()
    needs_key = any(r.strategy == MaskStrategy.TOKENIZE for r in rules)
    key = _require_key() if needs_key else key

    for rule in rules:
        if rule.column not in out.columns:
            logger.warning("PII rule targets missing column %s — skipping", rule.column)
            continue

        if rule.strategy == MaskStrategy.DROP:
            out = out.drop(columns=[rule.column])
        elif rule.strategy == MaskStrategy.MASK_FULL:
            out[rule.column] = out[rule.column].map(
                lambda v: "" if pd.isna(v) else "X" * len(str(v))
            )
        elif rule.strategy == MaskStrategy.MASK_PARTIAL_LAST4:

            def partial(v: Any) -> Any:
                if v is None or pd.isna(v):
                    return v
                s = str(v)
                return "X" * max(0, len(s) - 4) + s[-4:]

            out[rule.column] = out[rule.column].map(partial)
        elif rule.strategy == MaskStrategy.TOKENIZE:
            assert key is not None
            out[rule.column] = out[rule.column].map(
                lambda v: None if v is None or pd.isna(v) else tokenize_value(v, key)
            )
    return out


# ---------------------------------------------------------------------------
# Spark path (vectorized SQL expressions — no Python UDFs in the hot path)
# ---------------------------------------------------------------------------

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def mask_spark(df: Any, rules: List[PiiRule], key: Optional[str] = None) -> Any:
    """Apply PII rules to a PySpark DataFrame using native functions."""
    from pyspark.sql import functions as F

    needs_key = any(r.strategy == MaskStrategy.TOKENIZE for r in rules)
    pepper = (_require_key() if needs_key else key) or ""

    out = df
    for rule in rules:
        # Columns are interpolated into SQL expressions below — enforce plain
        # identifiers so rule files can never inject expressions.
        if not _IDENTIFIER_RE.match(rule.column):
            raise ValueError(
                f"PII rule column {rule.column!r} must be a plain identifier "
                "(letters/digits/underscore)."
            )
        if rule.column not in out.columns:
            logger.warning("PII rule targets missing column %s — skipping", rule.column)
            continue

        col = F.col(rule.column)
        c = rule.column
        if rule.strategy == MaskStrategy.DROP:
            out = out.drop(rule.column)
        elif rule.strategy == MaskStrategy.MASK_FULL:
            out = out.withColumn(
                rule.column,
                F.when(col.isNull(), F.lit(None)).otherwise(F.expr(f"repeat('X', length({c}))")),
            )
        elif rule.strategy == MaskStrategy.MASK_PARTIAL_LAST4:
            masked_expr = f"concat(repeat('X', greatest(length({c}) - 4, 0)), right({c}, 4))"
            out = out.withColumn(
                rule.column,
                F.when(col.isNull(), F.lit(None)).otherwise(F.expr(masked_expr)),
            )
        elif rule.strategy == MaskStrategy.TOKENIZE:
            token = F.sha2(F.concat(F.lit(pepper), col.cast("string")), 256)
            token = F.substring(token, 1, TOKEN_LENGTH_CHARS)
            out = out.withColumn(rule.column, F.when(col.isNull(), F.lit(None)).otherwise(token))
    return out
