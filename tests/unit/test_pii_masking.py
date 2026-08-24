"""Tests for src/processing/pii_masking.py (pandas path; no Spark needed)."""

import pytest

from src.processing.pii_masking import (
    KEY_ENV_VAR,
    MaskStrategy,
    PiiRule,
    mask_pandas,
    parse_rules,
    tokenize_value,
)

RULES = [
    PiiRule(column="email", strategy=MaskStrategy.TOKENIZE),
    PiiRule(column="card", strategy=MaskStrategy.MASK_PARTIAL_LAST4),
    PiiRule(column="ssn", strategy=MaskStrategy.MASK_FULL),
    PiiRule(column="internal_notes", strategy=MaskStrategy.DROP),
]


@pytest.fixture()
def df():
    import pandas as pd

    return pd.DataFrame(
        {
            "email": ["alice@example.com", "bob@example.com", None],
            "card": ["4111111111111111", "5500005555555559", None],
            "ssn": ["123-45-6789", "987-65-4321", ""],
            "internal_notes": ["a", "b", "c"],
            "amount": [1.0, 2.0, 3.0],
        }
    )


@pytest.fixture()
def key(monkeypatch):
    monkeypatch.setenv(KEY_ENV_VAR, "test-pepper-key")
    return "test-pepper-key"


class TestParseRules:
    def test_unknown_strategy_raises(self):
        with pytest.raises(ValueError, match="Unknown PII strategy"):
            parse_rules([{"column": "x", "strategy": "burn"}])

    def test_missing_keys_raise(self):
        with pytest.raises(ValueError, match="strategy"):
            parse_rules([{"column": "x"}])


class TestTokenize:
    def test_deterministic_for_joins(self, key):
        assert tokenize_value("alice@example.com", key) == tokenize_value("alice@example.com", key)
        assert tokenize_value("alice@example.com", key) != tokenize_value("bob@example.com", key)

    def test_key_changes_tokens(self):
        assert tokenize_value("v", "key-a") != tokenize_value("v", "key-b")

    def test_token_length_and_charset(self, key):
        token = tokenize_value("x", key)
        assert len(token) == 32
        assert all(c in "0123456789abcdef" for c in token)

    def test_tokenize_without_key_fails_fast(self, df, monkeypatch):
        monkeypatch.delenv(KEY_ENV_VAR, raising=False)
        with pytest.raises(ValueError, match=KEY_ENV_VAR):
            mask_pandas(df, RULES)


class TestMaskPandas:
    def test_full_pipeline(self, df, key):
        out = mask_pandas(df.copy(), RULES)

        # tokenize: deterministic, plaintext gone, nulls stay null
        assert out.loc[0, "email"] == tokenize_value("alice@example.com", key)
        assert "alice" not in out["email"].astype(str).tolist()[0]
        assert pd_isna(out.loc[2, "email"])

        # partial: length preserved, last 4 visible
        assert out.loc[0, "card"] == "XXXXXXXXXXXX1111"
        assert str(out.loc[0, "card"]).endswith("1111")
        assert pd_isna(out.loc[2, "card"])

        # full mask preserves length incl. dashes
        assert out.loc[0, "ssn"] == "XXXXXXXXXXX"

        # drop removes column; business columns untouched
        assert "internal_notes" not in out.columns
        assert out["amount"].tolist() == [1.0, 2.0, 3.0]

    def test_short_values_partial_mask(self, df, key):
        rules = [PiiRule(column="card", strategy=MaskStrategy.MASK_PARTIAL_LAST4)]
        df2 = df.copy()
        df2.loc[0, "card"] = "123"
        out = mask_pandas(df2, rules)
        assert out.loc[0, "card"] == "123"  # nothing to hide beyond last-4 window

    def test_missing_column_warns_not_crashes(self, df, key, caplog):
        rules = [PiiRule(column="ghost", strategy=MaskStrategy.MASK_FULL)]
        out = mask_pandas(df, rules)  # must not raise
        assert list(out.columns) == list(df.columns)


def pd_isna(v):
    import pandas as pd

    return pd.isna(v)
