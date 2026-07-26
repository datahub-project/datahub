from typing import Dict, Iterable, Mapping, Optional

from datahub.ingestion.agent.models import ProbeNode, ProbeResult


def assert_verdicts(
    result: ProbeResult,
    *,
    included: Iterable[str] = (),
    excluded: Mapping[str, str] = {},
) -> None:
    """Assert a ProbeResult's nodes carry the expected include/exclude
    verdicts, keyed by name.

    Every probe-capable connector's own test suite re-derives this by hand: a
    fake client exercising a level, then asserting an allowed object comes
    back `included: True` and a denied one comes back `included: False` with
    the right `excluded_by` (see probe_interface.md's "Testing expectations").
    This is the one check that generalises cleanly across every connector's
    probe tests checked so far (kafka, snowflake, bigquery, mode) -- the
    per-node kind/fqn/pattern_field assertions around it stay in the calling
    test, since those vary by connector and aren't this helper's job.

    `included` names nodes expected to be `included=True`; `excluded` maps a
    name to its expected `excluded_by` reason.
    """
    by_name: Dict[str, ProbeNode] = {node.name: node for node in result.nodes}
    for name in included:
        assert name in by_name, f"expected {name!r} in result nodes {sorted(by_name)}"
        node = by_name[name]
        assert node.included is True, f"{name!r} expected included, got {node}"
    for name, reason in excluded.items():
        assert name in by_name, f"expected {name!r} in result nodes {sorted(by_name)}"
        node = by_name[name]
        assert node.included is False, f"{name!r} expected excluded, got {node}"
        assert node.excluded_by == reason, (
            f"{name!r} excluded_by={node.excluded_by!r}, expected {reason!r}"
        )


def assert_degrades_with_warning(
    result: ProbeResult, *, contains: Optional[str] = None
) -> None:
    """Assert a ProbeResult reports the degrade path: a sub-listing that hit
    a soft error (see agent.probe.ProbeSoftError / soft_on_status)
    contributed nothing, but left a warning behind rather than looking like a
    silently empty level (see probe_interface.md's "Testing expectations").
    `contains`, when given, must appear in at least one warning (e.g. "404"
    or the name of the endpoint that failed).

    Setup stays per-connector -- there is no one way to make a fake client
    return a soft error (an HTTP session double, a SQLAlchemy inspector
    double, a Kafka consumer double, ...); this only standardises the
    assertion once that setup has already produced a ProbeResult.
    """
    assert result.warnings, f"expected at least one warning, got none: {result}"
    if contains is not None:
        assert any(contains in w for w in result.warnings), (
            f"no warning contains {contains!r}: {result.warnings}"
        )
