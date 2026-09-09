import logging
import re
from typing import Dict, List

import pytest

from conftest import _ingest_cleanup_data_impl
from tests.utilities.domains import Domain
from tests.utilities.metadata_operations import get_highlights

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.no_cypress_suite1, pytest.mark.domain(Domain.PLATFORM)]


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    """Guarantees at least one dataset carrying owners, tags, terms and a description.

    The highlights panel aggregates over whatever the instance happens to hold, so without
    this the entity-card and facet assertions would depend on data other suites ingested -
    passing or failing for reasons unrelated to the resolver.
    """
    yield from _ingest_cleanup_data_impl(
        auth_session, graph_client, "tests/analytics/data.json", "analytics_highlights"
    )


# Appended unconditionally by GetHighlightsResolver, so their absence means the
# resolver bailed out rather than that the instance has no data.
ACTIVE_USER_TITLES = ["Weekly Active Users", "Monthly Active Users"]

# Titles of the entity metadata-statistics cards. Each is emitted only when its
# entity type has at least one document.
ENTITY_TITLES = ["Datasets", "Dashboards", "Charts", "Pipelines", "Tasks", "Domains"]

DOMAIN_TITLE = "Domains"
DOMAIN_ASSIGNED_TEXT = "have domain assigned"

PERCENT_RE = re.compile(r"(\d+\.\d{2})%")


def _by_title(highlights: List[Dict]) -> Dict[str, Dict]:
    return {h["title"]: h for h in highlights}


def _percentages(body: str) -> List[float]:
    return [float(p) for p in PERCENT_RE.findall(body)]


def test_highlights_resolver_completes(auth_session, analytics_events_loaded):
    """The resolver swallows every exception into an empty list, so a bare
    not-None check cannot distinguish success from total failure. The two
    active-user cards are added unconditionally - if they are missing, the
    resolver threw."""
    highlights = get_highlights(auth_session)
    by_title = _by_title(highlights)

    for expected in ACTIVE_USER_TITLES:
        assert expected in by_title, (
            f"'{expected}' missing from highlights {sorted(by_title)}. This card is added "
            f"unconditionally, so its absence means getHighlights threw and returned []."
        )

    # Presence alone cannot distinguish a working query from one silently returning zeros,
    # since both cards are appended regardless. The fixture backfills ~45 days of usage
    # events, so at least one window must report active users.
    counts = {t: by_title[t]["value"] for t in ACTIVE_USER_TITLES}
    assert any(v > 0 for v in counts.values()), (
        f"Every active-user window reported zero: {counts}. Usage events were backfilled, "
        f"so the batched cardinality aggregation is likely not being read back correctly."
    )


def test_entity_highlights_are_present(auth_session, analytics_events_loaded):
    """Entity cards come from per-entity-type buckets in a single multi-index
    aggregation. If bucket keying breaks - for example if index aliases stop
    resolving - every bucket reads as zero and all of these cards silently
    vanish while the request still succeeds."""
    highlights = get_highlights(auth_session)
    by_title = _by_title(highlights)

    present = [t for t in ENTITY_TITLES if t in by_title]
    assert present, (
        f"No entity metadata highlights returned. Got {sorted(by_title)}. "
        f"Entity cards are dropped when their bucket count is 0, so all of them "
        f"disappearing points at the per-entity aggregation, not at empty indices."
    )
    logger.info(f"Entity highlights present: {present}")

    for title in present:
        assert by_title[title]["value"] > 0, (
            f"'{title}' was emitted with value {by_title[title]['value']}; "
            f"zero-count entity types should be omitted entirely."
        )


def test_entity_highlight_percentages_are_within_bounds(
    auth_session, analytics_events_loaded
):
    """Facet counts are a sub-aggregation nested inside each entity-type bucket.
    If they were ever computed outside that bucket they would count documents
    across all indices, producing percentages above 100."""
    highlights = get_highlights(auth_session)
    by_title = _by_title(highlights)

    checked = 0
    for title in ENTITY_TITLES:
        if title not in by_title:
            continue
        body = by_title[title]["body"]
        percentages = _percentages(body)
        assert percentages, f"'{title}' body carried no percentages: {body!r}"

        for pct in percentages:
            assert 0.0 <= pct <= 100.0, (
                f"'{title}' reported {pct}% in {body!r}. A facet count exceeding the "
                f"entity total means facet filters are not scoped to the entity bucket."
            )
        checked += 1

    assert checked > 0, "No entity highlights available to check"


def test_entity_highlight_body_shape(auth_session, analytics_events_loaded):
    """Domains deliberately omit the 'has domain' percentage. Getting this
    backwards for any type would mean entity buckets are keyed to the wrong
    entity type."""
    highlights = get_highlights(auth_session)
    by_title = _by_title(highlights)

    checked = 0
    for title in ENTITY_TITLES:
        if title not in by_title:
            continue
        checked += 1
        body = by_title[title]["body"]

        if title == DOMAIN_TITLE:
            assert DOMAIN_ASSIGNED_TEXT not in body, (
                f"Domains should not report a domain percentage, got {body!r}"
            )
            assert len(_percentages(body)) == 4, (
                f"Domains should report 4 percentages, got {body!r}"
            )
        else:
            assert DOMAIN_ASSIGNED_TEXT in body, (
                f"'{title}' should report a domain percentage, got {body!r}"
            )
            assert len(_percentages(body)) == 5, (
                f"'{title}' should report 5 percentages, got {body!r}"
            )

    assert checked > 0, "No entity highlights available to check"


def test_entity_highlight_facets_are_populated(auth_session, analytics_events_loaded):
    """Missing facet buckets degrade to 0 by design, which renders as a card with a
    correct total but every percentage at 0.00% - indistinguishable from 'nothing is
    annotated' unless annotated metadata is known to exist. The module fixture ingests a
    dataset with owners, tags and terms, so at least one non-zero percentage is
    guaranteed regardless of what else the instance holds."""
    highlights = get_highlights(auth_session)
    by_title = _by_title(highlights)

    all_percentages: Dict[str, List[float]] = {
        title: _percentages(by_title[title]["body"])
        for title in ENTITY_TITLES
        if title in by_title
    }
    assert all_percentages, "No entity highlights available to check"

    logger.info(f"Entity facet percentages: {all_percentages}")

    assert any(any(p > 0 for p in pcts) for pcts in all_percentages.values()), (
        f"Every facet percentage across every entity type was 0.00%: {all_percentages}. "
        f"Entity totals are non-zero, so the facet sub-aggregation is likely not being "
        f"read back correctly."
    )
