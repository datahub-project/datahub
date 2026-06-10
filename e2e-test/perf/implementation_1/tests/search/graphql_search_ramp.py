"""
Load test: Global Search — GraphQL searchAcrossEntities
Scenario:  ramp (gradually increase 50 → 200 users to find the knee)
Endpoint:  POST /api/graphql

Pre-requisite — seed the data first:
    python datapacks/seed_search.py --count 1000000 --host http://localhost:8080

Run (web UI — from e2e-test/perf/implementation_1/):
    locust -f tests/search/graphql_search_ramp.py -H http://localhost:8080

Run (headless — from e2e-test/perf/implementation_1/):
    locust -f tests/search/graphql_search_ramp.py \\
        --headless -H http://localhost:8080 \\
        -u 200 -r 10 -t 10m \\
        --csv baselines/search-ramp-$(date +%Y%m%d)

Auth:
    export DATAHUB_GMS_TOKEN=<token>   # picked up automatically by DataHubUser
    (omit if running against a local no-auth dev instance)

Manifest:
    The test reads seed-output/manifest.json at startup to use real seeded URNs.
    Falls back to random URN generation if no manifest is present.

Requirements (from repo root):
    pip install -r e2e-test/perf/implementation_1/requirements.txt
"""

import json
import random
import sys
from pathlib import Path
from typing import Any

from locust import between, task

# Ensure implementation_1/ is on sys.path so `tests.framework` is importable
# regardless of the working directory locust is invoked from.
_IMPL1_ROOT = Path(__file__).resolve().parents[2]
if str(_IMPL1_ROOT) not in sys.path:
    sys.path.insert(0, str(_IMPL1_ROOT))

from tests.framework.base_perf_test import DataHubUser, random_dataset_urn  # noqa: E402

# ── Load seeded URNs from manifest (written by seed_search.py) ───────────────
_MANIFEST_PATH = Path(__file__).parent.parent.parent.parent / "seed-output" / "manifest.json"

_SEEDED_URNS: list[str] = []
_SEEDED_TAGS: list[str] = []
_SEEDED_PLATFORMS: list[str] = []

def _load_manifest() -> None:
    global _SEEDED_URNS, _SEEDED_TAGS, _SEEDED_PLATFORMS
    if _MANIFEST_PATH.exists():
        try:
            data = json.loads(_MANIFEST_PATH.read_text())
            _SEEDED_URNS = data.get("sample_urns", {}).get("dataset", [])
            vocab = data.get("vocabulary", {})
            _SEEDED_TAGS = vocab.get("tags", [])
            _SEEDED_PLATFORMS = vocab.get("platforms", [])
            print(
                f"[manifest] Loaded {len(_SEEDED_URNS)} URNs, "
                f"{len(_SEEDED_TAGS)} tags from {_MANIFEST_PATH}"
            )
        except Exception as exc:
            print(f"[manifest] WARNING: could not load {_MANIFEST_PATH}: {exc}")
    else:
        print(
            f"[manifest] {_MANIFEST_PATH} not found — "
            "using random URNs (run seed_search.py first for realistic results)"
        )

_load_manifest()

# ── realistic search query pool ───────────────────────────────────────────────
# Mirrors what real users type: short keywords, wildcards, dataset names
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
    ["DATASET"],                              # most common — datasets only
    ["DATASET", "DASHBOARD"],                 # data + BI assets
    ["DATASET", "CHART"],
    ["DATASET", "DATA_FLOW", "DATA_JOB"],     # pipeline discovery
    ["DASHBOARD", "CHART"],
    [],                                       # all types (global search, heaviest)
]

# GraphQL operation strings — kept as module constants to avoid per-request alloc
_SEARCH_ACROSS_ENTITIES = """
query searchAcrossEntities($input: SearchAcrossEntitiesInput!) {
  searchAcrossEntities(input: $input) {
    start
    count
    total
    searchResults {
      entity {
        urn
        type
      }
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
      aggregations {
        value
        count
      }
    }
    searchResults {
      entity {
        urn
        type
      }
      matchedFields {
        name
        value
      }
    }
  }
}
"""

_AUTOCOMPLETE = """
query autoCompleteForMultiple($input: AutoCompleteMultipleInput!) {
  autoCompleteForMultiple(input: $input) {
    query
    suggestions {
      type
      suggestions
    }
  }
}
"""


class GlobalSearchUser(DataHubUser):
    """
    Simulates users performing global search across DataHub entities.

    Task mix (weights):
      5 — bare keyword search (most frequent real-world pattern)
      3 — typed search with entity type filter
      2 — faceted search (returns aggregations — heavier)
      1 — autocomplete (fast typeahead path)

    wait_time: between(1, 3) — ramp scenario, realistic think-time between searches.

    URNs come from manifest.json written by seed_search.py.
    Falls back to random generation if manifest is absent.
    """

    wait_time = between(1, 3)

    def on_start(self) -> None:
        super().on_start()
        # Each worker gets its own shuffled copy so workers don't step on each other
        self._urns: list[str] = list(_SEEDED_URNS) if _SEEDED_URNS else []
        self._tags: list[str] = list(_SEEDED_TAGS) if _SEEDED_TAGS else list(
            t.replace("-", " ") for t in [
                "pii", "financial", "customer-data", "certified", "deprecated"
            ]
        )
        random.shuffle(self._urns)

    def _search_term(self) -> str:
        """Return a realistic search term: either from the preset pool or a seeded keyword."""
        if self._urns and random.random() < 0.3:
            # 30% of the time: extract a keyword from a real seeded table name
            urn = random.choice(self._urns)
            # urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.raw.perf_search_orders_fact_000001,PROD)"
            try:
                table_part = urn.split(",")[1].rsplit(".", 1)[-1]
                # strip perf_search_ prefix and suffix
                word = table_part.replace("perf_search_", "").split("_")[0]
                if word:
                    return word
            except (IndexError, AttributeError):
                pass
        return random.choice(_SEARCH_TERMS)

    # ── tasks ─────────────────────────────────────────────────────────────────

    @task(5)
    def search_keyword(self) -> None:
        """Bare keyword search across all entity types — the lightest, most common path."""
        term = self._search_term()
        self.graphql(
            query=_SEARCH_ACROSS_ENTITIES,
            variables={
                "input": {
                    "query": term,
                    "start": 0,
                    "count": 10,
                    "searchFlags": {
                        "skipAggregates": True,
                        "skipHighlighting": False,
                        "fulltext": True,
                    },
                }
            },
            name="graphql/searchAcrossEntities [keyword]",
        )

    @task(3)
    def search_typed(self) -> None:
        """Keyword search restricted to a specific entity type mix."""
        term = self._search_term()
        types = random.choice(_ENTITY_TYPE_MIXES[:-1])  # exclude all-types here
        variables: dict[str, Any] = {
            "input": {
                "query": term,
                "start": random.randint(0, 3) * 10,  # paginate across first 4 pages
                "count": 10,
                "searchFlags": {
                    "skipAggregates": True,
                    "fulltext": True,
                },
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
        """
        Full search including aggregation facets — the heaviest read path.
        Mirrors the DataHub UI's main search results page (facet sidebar).
        """
        term = self._search_term()
        types = random.choice(_ENTITY_TYPE_MIXES)
        variables: dict[str, Any] = {
            "input": {
                "query": term,
                "start": 0,
                "count": 10,
                "searchFlags": {
                    "skipAggregates": False,
                    "fulltext": True,
                },
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
        """
        Typeahead autocomplete — fired on every keystroke in the UI.
        Uses a partial 3-character prefix to simulate real typing behaviour.
        """
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
