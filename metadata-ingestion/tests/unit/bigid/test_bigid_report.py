"""Unit tests for _rank_to_float (lives in bigid_utils)."""

from datahub.ingestion.source.bigid.bigid_utils import _rank_to_float

# ---------------------------------------------------------------------------
# _rank_to_float
# ---------------------------------------------------------------------------


def test_rank_to_float_high():
    assert _rank_to_float("HIGH") == 0.75


def test_rank_to_float_medium():
    assert _rank_to_float("MEDIUM") == 0.50


def test_rank_to_float_low():
    assert _rank_to_float("LOW") == 0.25


def test_rank_to_float_unknown():
    assert _rank_to_float("UNKNOWN") == 0.0


def test_rank_to_float_case_insensitive():
    assert _rank_to_float("high") == 0.75
