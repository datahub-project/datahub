from types import SimpleNamespace

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.mode_probe import MODE_PROBE, list_mode_children


class _FakeSession:
    """Answers the four Mode endpoints the probe uses."""

    def __init__(self):
        self.calls = []

    def get(self, url, **kw):
        self.calls.append(url)
        if url.endswith("/spaces"):
            body = {
                "spaces": [
                    {"name": "Personal", "token": "sp1"},
                    {"name": "Archive", "token": "sp2"},
                ]
            }
        elif url.endswith("/reports"):
            body = {"reports": [{"name": "Weekly", "token": "r1"}]}
        elif url.endswith("/datasets"):
            body = {"datasets": [{"name": "Seed", "token": "d1"}]}
        elif url.endswith("/queries"):
            body = {"queries": [{"name": "q_main", "token": "q1"}]}
        else:
            body = {}
        return SimpleNamespace(
            ok=True, status_code=200, json=lambda: {"_embedded": body}
        )


def _cfg(**over):
    base = dict(
        space_pattern=AllowDenyPattern(allow=[".*"], deny=["^Archive$"]),
        report_pattern=AllowDenyPattern.allow_all(),
    )
    base.update(over)
    session = _FakeSession()
    return SimpleNamespace(
        get_mode_session=lambda: (session, "https://app.mode.com/api/acryltest"),
        **base,
    )


def test_mode_shape_branches_under_space():
    shape = MODE_PROBE.shape().to_dict()
    assert shape["kind"] == "Space"
    # A Space holds BOTH reports and datasets — the branch.
    assert sorted(c["kind"] for c in shape["children"]) == ["Dataset", "Report"]
    assert MODE_PROBE.is_linear is False


def test_spaces_apply_space_pattern():
    by_name = {n.name: n for n in list_mode_children(_cfg(), [], 100).nodes}
    assert by_name["Personal"].included is True
    assert by_name["Personal"].pattern_field == "space_pattern"
    assert by_name["Archive"].included is False
    assert by_name["Archive"].excluded_by == "space_pattern"


def test_listing_a_space_merges_reports_and_datasets():
    nodes = list_mode_children(_cfg(), ["Personal"], 100).nodes
    kinds = {n.name: str(n.kind) for n in nodes}
    assert kinds == {"Weekly": "Report", "Seed": "Dataset"}
    # Reports are filterable; datasets are not (Mode offers no dataset_pattern).
    by_name = {n.name: n for n in nodes}
    assert by_name["Weekly"].pattern_field == "report_pattern"
    assert by_name["Seed"].pattern_field is None


def test_descending_into_a_report_needs_a_qualifier():
    with pytest.raises(ValueError, match="ambiguous|Report:"):
        list_mode_children(_cfg(), ["Personal", "Weekly"], 100)


def test_qualified_descent_lists_queries():
    nodes = list_mode_children(_cfg(), ["Personal", "Report:Weekly"], 100).nodes
    assert [n.name for n in nodes] == ["q_main"]
