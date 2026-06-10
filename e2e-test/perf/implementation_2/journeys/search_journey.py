"""
GlobalSearchJourney — Grasshopper journey for DataHub global search load tests.

Scenario: ramp users to find the throughput knee on searchAcrossEntities.

Task mix (weights):
  5 — bare keyword search     (lightest, most common UI pattern)
  3 — typed entity search     (type filter + pagination)
  2 — faceted search          (full aggregations, heaviest read path)
  1 — autocomplete            (typeahead, fast path)
  1 — search_flow trend       (multi-step composite journey, recorded as a custom trend)

The @custom_trend("search_flow") task measures the full user session
(keyword → facets → autocomplete) as a single timing metric, enabling
threshold enforcement on the end-to-end journey, not just individual requests.
"""

import random
from typing import Any

from grasshopper.lib.util.utils import custom_trend
from locust import between, task

from framework.datahub_journey import DataHubJourney

_SEARCH_TERMS = [
    "*",
    "test",
    "sales",
    "revenue",
    "orders",
    "customers",
    "events",
    "clicks",
    "pageviews",
    "sessions",
    "users",
    "transactions",
    "inventory",
    "products",
    "shipping",
    "returns",
    "finance",
    "marketing",
    "pipeline",
    "raw",
    "staging",
    "prod",
    "daily",
    "weekly",
]

_ENTITY_TYPE_MIXES: list[list[str]] = [
    ["DATASET"],
    ["DATASET", "DASHBOARD"],
    ["DATASET", "CHART"],
    ["DATASET", "DATA_FLOW", "DATA_JOB"],
    ["DASHBOARD", "CHART"],
    [],  # all types — heaviest
]

_SEARCH_ACROSS_ENTITIES = """
query searchAcrossEntities($input: SearchAcrossEntitiesInput!) {
  searchAcrossEntities(input: $input) {
    start
    count
    total
    searchResults {
      entity { urn type }
    }
  }
}
"""

_SEARCH_ACROSS_ENTITIES_WITH_FILTERS = """
query searchAcrossEntitiesFiltered($input: SearchAcrossEntitiesInput!) {
  searchAcrossEntities(input: $input) {
    start
    count
    total
    facets {
      field
      aggregations { value count }
    }
    searchResults {
      entity { urn type }
      matchedFields { name value }
    }
  }
}
"""

_AUTOCOMPLETE = """
query autoCompleteForMultiple($input: AutoCompleteMultipleInput!) {
  autoCompleteForMultiple(input: $input) {
    query
    suggestions { type suggestions }
  }
}
"""


class GlobalSearchJourney(DataHubJourney):
    """
    Grasshopper journey simulating users performing global search in DataHub.

    Configuration injected via YAML scenario file (search_scenarios.yaml):
      - users / spawn_rate / runtime   → test_run section
      - thresholds                     → scenario section (p90 gates per request + trends)
      - gms_token                      → scenario section (overrides DATAHUB_GMS_TOKEN env var)

    Thresholds example (in YAML scenario):
      thresholds:
        graphql/searchAcrossEntities [keyword]: {type: post, limit: 2000}
        search_flow: {type: custom, limit: 10000}
    """

    wait_time = between(1, 3)
    defaults = {
        "gms_token": "",
        "entity_types": "DATASET",
    }

    def on_start(self) -> None:
        super().on_start()
        self._urns: list[str] = list(self._manifest_urns)
        self._tags: list[str] = list(self._manifest_tags) or [
            "pii",
            "financial",
            "customer-data",
            "certified",
            "deprecated",
        ]
        random.shuffle(self._urns)

    def _search_term(self) -> str:
        """Return a realistic search term: preset pool, or a keyword from a seeded table name."""
        if self._urns and random.random() < 0.3:
            try:
                urn = random.choice(self._urns)
                table_part = urn.split(",")[1].rsplit(".", 1)[-1]
                word = table_part.replace("perf_search_", "").split("_")[0]
                if word:
                    return word
            except (IndexError, AttributeError):
                pass
        return random.choice(_SEARCH_TERMS)

    # ── tasks ─────────────────────────────────────────────────────────────────

    @task(5)
    def search_keyword(self) -> None:
        """Bare keyword search across all entity types — lightest, most common path."""
        self.graphql(
            query=_SEARCH_ACROSS_ENTITIES,
            variables={
                "input": {
                    "query": self._search_term(),
                    "start": 0,
                    "count": 10,
                    "searchFlags": {"skipAggregates": True, "fulltext": True},
                }
            },
            name="graphql/searchAcrossEntities [keyword]",
        )

    @task(3)
    def search_typed(self) -> None:
        """Keyword search restricted to a specific entity type mix, with pagination."""
        types = random.choice(_ENTITY_TYPE_MIXES[:-1])
        variables: dict[str, Any] = {
            "input": {
                "query": self._search_term(),
                "start": random.randint(0, 3) * 10,
                "count": 10,
                "searchFlags": {"skipAggregates": True, "fulltext": True},
            }
        }
        if types:
            variables["input"]["types"] = types
        self.graphql(
            query=_SEARCH_ACROSS_ENTITIES,
            variables=variables,
            name="graphql/searchAcrossEntities [typed]",
        )

    @task(2)
    def search_with_facets(self) -> None:
        """Full search with aggregation facets — mirrors the DataHub UI results page."""
        types = random.choice(_ENTITY_TYPE_MIXES)
        variables: dict[str, Any] = {
            "input": {
                "query": self._search_term(),
                "start": 0,
                "count": 10,
                "searchFlags": {"skipAggregates": False, "fulltext": True},
            }
        }
        if types:
            variables["input"]["types"] = types
        self.graphql(
            query=_SEARCH_ACROSS_ENTITIES_WITH_FILTERS,
            variables=variables,
            name="graphql/searchAcrossEntities [with-facets]",
        )

    @task(1)
    def autocomplete(self) -> None:
        """Typeahead autocomplete — 3-char prefix simulating real typing behaviour."""
        term = random.choice(_SEARCH_TERMS)
        prefix = term[:3] if len(term) >= 3 else term
        self.graphql(
            query=_AUTOCOMPLETE,
            variables={
                "input": {
                    "query": prefix,
                    "limit": 5,
                    "types": ["DATASET", "DASHBOARD", "CHART"],
                }
            },
            name="graphql/autoCompleteForMultiple",
        )

    @task(1)
    @custom_trend("search_flow", extra_tag_keys=["entity_types"])
    def search_flow(self) -> None:
        """
        Multi-step search session: keyword → faceted refinement → autocomplete.

        Wrapped with @custom_trend so the end-to-end duration is recorded as
        a single CUSTOM/search_flow metric, enabling threshold enforcement on
        the composite user journey (not just individual requests).

        record_check=False on each sub-call because the trend itself captures
        overall health; individual checks at this level create noise.
        """
        term = self._search_term()

        self.graphql(
            query=_SEARCH_ACROSS_ENTITIES,
            variables={
                "input": {
                    "query": term,
                    "start": 0,
                    "count": 10,
                    "searchFlags": {"skipAggregates": True, "fulltext": True},
                }
            },
            name="graphql/searchAcrossEntities [keyword]",
            record_check=False,
        )

        types = random.choice(_ENTITY_TYPE_MIXES[:-1])
        variables: dict[str, Any] = {
            "input": {
                "query": term,
                "start": 0,
                "count": 10,
                "searchFlags": {"skipAggregates": False, "fulltext": True},
            }
        }
        if types:
            variables["input"]["types"] = types
        self.graphql(
            query=_SEARCH_ACROSS_ENTITIES_WITH_FILTERS,
            variables=variables,
            name="graphql/searchAcrossEntities [with-facets]",
            record_check=False,
        )

        prefix_term = random.choice(_SEARCH_TERMS)
        prefix = prefix_term[:3] if len(prefix_term) >= 3 else prefix_term
        self.graphql(
            query=_AUTOCOMPLETE,
            variables={
                "input": {
                    "query": prefix,
                    "limit": 5,
                    "types": ["DATASET", "DASHBOARD", "CHART"],
                }
            },
            name="graphql/autoCompleteForMultiple",
            record_check=False,
        )
