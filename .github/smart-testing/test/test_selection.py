"""Tests for selection.py (run by .github/workflows/test-github-scripts.yml)."""

import json

import pytest

import selection


@pytest.mark.parametrize(
    ("smart_tags", "expected"),
    [
        ("", "p0"),
        ("  ", "p0"),
        ('{"feature-user": ["lineage"], "confidence": "HIGH"}', "smart"),
        ('{"compound": ["propagation:docs"], "confidence": "MEDIUM"}', "smart"),
        ('{"technical-layer": ["graphql"], "confidence": "HIGH"}', "canary"),
        ('{"feature-user": [], "confidence": "HIGH"}', "canary"),
        ('{"confidence": "HIGH"}', "canary"),
    ],
)
def test_selection_for(smart_tags: str, expected: str) -> None:
    assert selection.selection_for(smart_tags) == expected


def test_unreadable_tags_fail_the_plan_instead_of_guessing() -> None:
    # A failed plan step falls back to the fixed p0 matrix; a guess would not.
    with pytest.raises(json.JSONDecodeError):
        selection.selection_for("{not json")
    with pytest.raises(ValueError):
        selection.selection_for('["feature-user"]')


def test_main_reads_smoke_smart_tags(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    monkeypatch.setenv(
        "SMOKE_SMART_TAGS", '{"feature-user": ["lineage"], "confidence": "HIGH"}'
    )
    selection.main()
    assert capsys.readouterr().out == "smart\n"
