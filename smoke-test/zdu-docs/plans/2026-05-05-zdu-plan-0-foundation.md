# ZDU E2E — Plan 0: Foundation (Common Framework + Utilities)

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the common framework and utility groundwork that every subsequent phase plan (Plan 1–Plan 8) depends on. After this plan, no new pipeline phase exists — but the _scaffolding_ is in place: client abstractions, scenario-type dispatch, suite tagging, configuration plumbing. Suite A's existing 23 scenarios continue to pass and now route through the new registry, so the abstraction is exercised before Plan 1 lands.

**Architecture / patterns enforced:**

- **Single Responsibility** — one class per concern (`Suite`, `MySQLClient`, `ElasticsearchClient`, `ScenarioTypeRegistry`).
- **Dependency Injection** — clients are constructed once in `runner.py` and injected; phases never construct their own clients.
- **Strategy pattern** for scenario-type dispatch (`ScenarioTypeExecutor` protocol + registry). Future phase plans register a new executor without touching this module.
- **Type-first** — every public function has full type hints. No `Any` outside JSON-passthrough boundaries.
- **TDD** — every new module ships with a unit test file that runs without a live stack.
- **No premature abstractions** — phase-contract dataclasses (`SnapshotT0` etc.) are added by their _writer's_ plan, not pre-emptively here.

**Tech Stack:** Python 3, pytest, `requests`, `pymysql` (verify availability in Task 1), Docker Compose v2.

**Out of scope:** Any new phase implementation; any new TC; any phase-contract dataclasses. This plan touches the existing 5-phase pipeline only to wire the new clients + registry. Suite A behaviour preserved.

**Code-review handoff:** Task 13 dispatches `feature-dev:code-reviewer` before merge.

---

## File Structure

```
smoke-test/tests/zdu/
├── conftest.py                                  (no change in this plan)
├── test_zdu_upgrade.py                          MODIFY — apply suite_<x> marker via pytest.param
├── pytest.ini                                   CREATE (if no project-level marker registry exists)
├── __main__.py                                  MODIFY — use from_env() with CLI overrides; --suite flag
├── framework/
│   ├── config.py                                MODIFY — add suites, es_url, mysql_*, *_delay_ms
│   ├── runner.py                                MODIFY — DI of MySQLClient + registry; suite filter
│   ├── scenario_loader.py                       MODIFY — scenario_type+suite fields; ScenarioExecutor takes ctx
│   ├── es_client.py                             REWRITE — direct-ES methods + retain GMS-mediated search
│   ├── phases/validation.py                     MODIFY — dispatch through registry
│   ├── mysql_client.py                          CREATE — pymysql-backed direct DB client
│   ├── suite.py                                 CREATE — Suite enum + suite_for_tc()
│   ├── scenario_executor.py                     CREATE — ScenarioTypeExecutor protocol + registry
│   ├── test_suite.py                            CREATE — unit tests
│   ├── test_mysql_client.py                     CREATE — unit tests
│   ├── test_es_client.py                        CREATE — unit tests
│   ├── test_scenario_executor.py                CREATE — unit tests
│   └── (test_log_monitor.py, test_scenario_loader.py — existing tests preserved; new cases appended)
```

**Note on the existing `ScenarioExecutor`** (in `scenario_loader.py`): it stays as the implementation for the `aspect_migration` strategy. Task 6 changes its `validate()` signature from `(scenario, urns)` to `(scenario, ctx)` so it can be registered directly into the new registry without an adapter.

---

## Task 1: Read-only dependency diagnostics

**Files:** none modified.

- [ ] **Step 1: Confirm `pymysql` and `requests` available in the smoke-test venv**

```bash
smoke-test/venv/bin/python -c "
import sys
try:
    import pymysql; print('pymysql', pymysql.__version__)
except ImportError:
    print('PYMYSQL_MISSING'); sys.exit(0)
import requests; print('requests', requests.__version__)
"
```

If output starts with `PYMYSQL_MISSING`, proceed to Task 2. Otherwise, skip Task 2.

- [ ] **Step 2: Establish baseline — existing tests pass**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ -v 2>&1 | tail -20
```

Expected: `test_log_monitor.py` (≥7 cases) and `test_scenario_loader.py` (≥1 case) all pass. Record the count — every later step re-runs these and they MUST stay green.

- [ ] **Step 3: Confirm direct-ES port (informational)**

```bash
grep -nE "9200|elasticsearch" <REPO_ROOT>/docker/profiles/docker-compose.prerequisites.yml | head -5
```

Expected: ES runs on `9200` with no auth in dev compose.

---

## Task 2: Add `pymysql` dependency (only if Task 1 reported `PYMYSQL_MISSING`)

**Skip this task entirely if Task 1 step 1 printed a `pymysql` version.**

**Files:**

- Modify: `smoke-test/setup.py`

- [ ] **Step 1: Locate `install_requires`**

```bash
grep -n "install_requires" smoke-test/setup.py
```

- [ ] **Step 2: Add `"pymysql"` to the `install_requires` list**

Use the `Edit` tool. Insert `"pymysql",` adjacent to `"requests"` to keep the section ordered.

- [ ] **Step 3: Reinstall the dev environment**

```bash
./gradlew :smoke-test:installDev
```

Expected: BUILD SUCCESSFUL.

- [ ] **Step 4: Verify import**

```bash
smoke-test/venv/bin/python -c "import pymysql; print(pymysql.__version__)"
```

- [ ] **Step 5: Commit**

```bash
git add smoke-test/setup.py
git commit -m "build(smoke-test): add pymysql dependency for ZDU framework"
```

---

## Task 3: `Suite` enum + `suite_for_tc()`

**Files:**

- Create: `smoke-test/tests/zdu/framework/suite.py`
- Create: `smoke-test/tests/zdu/framework/test_suite.py`

**Pattern:** A pure-data enum with one derived helper. No state, no dependencies.

- [ ] **Step 1: Write the failing test**

```python
# smoke-test/tests/zdu/framework/test_suite.py
"""Unit tests for the Suite enum and tc→suite mapping."""

from __future__ import annotations

import pytest

from tests.zdu.framework.suite import Suite, suite_for_tc


class TestSuiteEnum:
    def test_lowercase_string_values(self) -> None:
        assert Suite.A.value == "a"
        assert Suite.H.value == "h"

    def test_all_eight_suites_present(self) -> None:
        assert {s.value for s in Suite} == {"a", "b", "c", "d", "e", "f", "g", "h"}


class TestSuiteForTc:
    @pytest.mark.parametrize(
        "tc,expected",
        [
            (1, Suite.A), (23, Suite.A),
            (101, Suite.B), (112, Suite.B),
            (201, Suite.C), (208, Suite.C),
            (301, Suite.D), (309, Suite.D),
            (401, Suite.E), (408, Suite.E),
            (501, Suite.F), (507, Suite.F),
            (601, Suite.G), (604, Suite.G),
            (701, Suite.H), (705, Suite.H),
        ],
    )
    def test_known_ranges(self, tc: int, expected: Suite) -> None:
        assert suite_for_tc(tc) == expected

    def test_unknown_returns_none(self) -> None:
        assert suite_for_tc(999) is None
        assert suite_for_tc(0) is None
        assert suite_for_tc(-1) is None
```

- [ ] **Step 2: Run test — expect failure**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_suite.py -v
```

Expected: `ModuleNotFoundError: No module named 'tests.zdu.framework.suite'`.

- [ ] **Step 3: Implement**

```python
# smoke-test/tests/zdu/framework/suite.py
"""Suite identifiers for the ZDU end-to-end test framework.

Each suite groups TCs that exercise a related ZDU concern. The enum value is
the lowercase short code used as the pytest marker (``suite_a``, ``suite_b``,
...) and the ``--suite`` CLI argument.
"""

from __future__ import annotations

from enum import Enum


class Suite(Enum):
    A = "a"  # Aspect schema migration (TC-001..TC-023)
    B = "b"  # ES Phase 1 — incremental reindex (TC-101..TC-112)
    C = "c"  # Rollback dual-write (TC-201..TC-208)
    D = "d"  # ES Phase 2 catch-up (TC-301..TC-309)
    E = "e"  # System-level sweep (TC-401..TC-408)
    F = "f"  # Live traffic (TC-501..TC-507)
    G = "g"  # Rollback (TC-601..TC-604)
    H = "h"  # Failure recovery (TC-701..TC-705)


# Explicit mapping — keeps the source of truth in one place and avoids a
# silent off-by-one if a TC range shifts. Tests exercise the boundaries.
_TC_RANGES: tuple[tuple[range, Suite], ...] = (
    (range(1, 24), Suite.A),
    (range(101, 113), Suite.B),
    (range(201, 209), Suite.C),
    (range(301, 310), Suite.D),
    (range(401, 409), Suite.E),
    (range(501, 508), Suite.F),
    (range(601, 605), Suite.G),
    (range(701, 706), Suite.H),
)


def suite_for_tc(tc_number: int) -> Suite | None:
    """Return the Suite that owns ``tc_number``, or None if it falls outside known ranges."""
    for tc_range, suite in _TC_RANGES:
        if tc_number in tc_range:
            return suite
    return None
```

- [ ] **Step 4: Run test — expect pass**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_suite.py -v
```

Expected: 18 tests pass.

- [ ] **Step 5: Commit**

```bash
git add smoke-test/tests/zdu/framework/suite.py \
        smoke-test/tests/zdu/framework/test_suite.py
git commit -m "feat(zdu): Suite enum and tc→suite mapping"
```

---

## Task 4: `MySQLClient`

**Files:**

- Create: `smoke-test/tests/zdu/framework/mysql_client.py`
- Create: `smoke-test/tests/zdu/framework/test_mysql_client.py`

**Pattern:** Connection-per-call (`@contextmanager`), `DictCursor`, `autocommit=True`. Defensive parsers — null/malformed inputs return `None`, never raise.

- [ ] **Step 1: Write the failing test**

```python
# smoke-test/tests/zdu/framework/test_mysql_client.py
"""Unit tests for MySQLClient — uses mocked cursors; never touches a real DB."""

from __future__ import annotations

import json
from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest

from tests.zdu.framework.mysql_client import EbeanAspectV2Row, MySQLClient


@pytest.fixture
def cursor() -> MagicMock:
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    return cur


@pytest.fixture
def client(cursor: MagicMock) -> Iterator[MySQLClient]:
    """Yield a MySQLClient whose connections always return ``cursor``."""
    conn = MagicMock()
    conn.cursor.return_value = cursor
    with patch("tests.zdu.framework.mysql_client.pymysql.connect", return_value=conn):
        yield MySQLClient(host="h", port=1, user="u", password="p", database="d")


class TestGetSchemaVersion:
    def test_returns_int_when_present(self, client: MySQLClient, cursor: MagicMock) -> None:
        cursor.fetchone.return_value = {"systemmetadata": json.dumps({"schemaVersion": 3})}
        assert client.get_schema_version("urn:li:dataset:foo", "embed") == 3

    def test_returns_none_when_field_absent(self, client: MySQLClient, cursor: MagicMock) -> None:
        cursor.fetchone.return_value = {"systemmetadata": "{}"}
        assert client.get_schema_version("urn:li:dataset:foo", "embed") is None

    def test_returns_none_when_metadata_is_null(self, client: MySQLClient, cursor: MagicMock) -> None:
        cursor.fetchone.return_value = {"systemmetadata": None}
        assert client.get_schema_version("urn:li:dataset:foo", "embed") is None

    def test_returns_none_when_no_row(self, client: MySQLClient, cursor: MagicMock) -> None:
        cursor.fetchone.return_value = None
        assert client.get_schema_version("urn:li:dataset:foo", "embed") is None

    def test_returns_none_on_malformed_json(self, client: MySQLClient, cursor: MagicMock) -> None:
        cursor.fetchone.return_value = {"systemmetadata": "{not json"}
        assert client.get_schema_version("urn:li:dataset:foo", "embed") is None


class TestGetAspectRaw:
    def test_returns_dataclass(self, client: MySQLClient, cursor: MagicMock) -> None:
        cursor.fetchone.return_value = {
            "urn": "urn:li:dataset:foo", "aspect": "embed", "version": 0,
            "metadata": "{}", "systemmetadata": "{}",
            "createdon": "2026-04-01 00:00:00", "createdby": "urn:li:corpuser:datahub",
        }
        row = client.get_aspect_raw("urn:li:dataset:foo", "embed")
        assert isinstance(row, EbeanAspectV2Row)
        assert row.urn == "urn:li:dataset:foo"

    def test_returns_none_when_missing(self, client: MySQLClient, cursor: MagicMock) -> None:
        cursor.fetchone.return_value = None
        assert client.get_aspect_raw("urn:li:dataset:foo", "embed") is None


class TestCountAspectsBySchemaVersion:
    def test_groups_by_schema_version(self, client: MySQLClient, cursor: MagicMock) -> None:
        cursor.fetchall.return_value = [
            {"v": None, "n": 5},
            {"v": 2, "n": 7},
            {"v": 4, "n": 3},
        ]
        assert client.count_aspects_by_schema_version("embed") == {None: 5, 2: 7, 4: 3}


class TestGetUpgradeResult:
    def test_returns_parsed_json(self, client: MySQLClient, cursor: MagicMock) -> None:
        cursor.fetchone.return_value = {"metadata": json.dumps({"indicesState": {}})}
        assert client.get_upgrade_result("system-update-blocking") == {"indicesState": {}}

    def test_returns_none_when_missing(self, client: MySQLClient, cursor: MagicMock) -> None:
        cursor.fetchone.return_value = None
        assert client.get_upgrade_result("system-update-blocking") is None

    def test_returns_none_on_malformed_json(self, client: MySQLClient, cursor: MagicMock) -> None:
        cursor.fetchone.return_value = {"metadata": "{not json"}
        assert client.get_upgrade_result("system-update-blocking") is None
```

- [ ] **Step 2: Run test — expect failure**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_mysql_client.py -v
```

Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement**

```python
# smoke-test/tests/zdu/framework/mysql_client.py
"""Direct MySQL client for ZDU phases.

Connection-per-call (single-thread test usage; no pool). Defaults match the
local Docker Compose dev profile: ``localhost:3306`` / ``datahub`` /
``datahub`` / ``datahub``.

Reads parse JSON columns and return strongly-typed values; null/malformed
inputs return ``None`` rather than raising — callers treat absence as
"the row exists in a pre-ZDU shape".

Note: ``count_aspects_by_schema_version`` uses MySQL's ``JSON_EXTRACT``,
which returns the JSON value directly. The pymysql DictCursor surfaces
``schemaVersion: null`` as Python ``None`` and integer values as ``int``.
This is verified by the integration sanity check in Task 12.
"""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator

import pymysql
import pymysql.cursors

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class EbeanAspectV2Row:
    """One row from ``metadata_aspect_v2`` (the canonical aspect table)."""
    urn: str
    aspect: str
    version: int
    metadata: str
    systemmetadata: str | None
    createdon: str
    createdby: str


class MySQLClient:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 3306,
        user: str = "datahub",
        password: str = "datahub",
        database: str = "datahub",
    ) -> None:
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database

    @contextmanager
    def _conn(self) -> Iterator[pymysql.connections.Connection]:
        conn = pymysql.connect(
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            database=self._database,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
        )
        try:
            yield conn
        finally:
            conn.close()

    def get_aspect_raw(self, urn: str, aspect: str) -> EbeanAspectV2Row | None:
        sql = (
            "SELECT urn, aspect, version, metadata, systemmetadata, "
            "createdon, createdby FROM metadata_aspect_v2 "
            "WHERE urn=%s AND aspect=%s AND version=0"
        )
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, (urn, aspect))
            row = cur.fetchone()
        return EbeanAspectV2Row(**row) if row else None

    def get_schema_version(self, urn: str, aspect: str) -> int | None:
        sql = (
            "SELECT systemmetadata FROM metadata_aspect_v2 "
            "WHERE urn=%s AND aspect=%s AND version=0"
        )
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, (urn, aspect))
            row = cur.fetchone()
        if not row or not row.get("systemmetadata"):
            return None
        try:
            meta = json.loads(row["systemmetadata"])
        except json.JSONDecodeError:
            return None
        v = meta.get("schemaVersion")
        return int(v) if v is not None else None

    def count_aspects_by_schema_version(self, aspect: str) -> dict[int | None, int]:
        sql = (
            "SELECT JSON_EXTRACT(systemmetadata, '$.schemaVersion') AS v, "
            "COUNT(*) AS n FROM metadata_aspect_v2 "
            "WHERE aspect=%s AND version=0 GROUP BY v"
        )
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, (aspect,))
            rows = cur.fetchall()
        return {row["v"]: row["n"] for row in rows}

    def get_upgrade_result(self, upgrade_id: str) -> dict | None:
        """Return the parsed ``DataHubUpgradeResult`` metadata for ``upgrade_id``, or None."""
        urn = f"urn:li:dataHubUpgrade:{upgrade_id}"
        sql = (
            "SELECT metadata FROM metadata_aspect_v2 "
            "WHERE urn=%s AND aspect='dataHubUpgradeResult' AND version=0"
        )
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, (urn,))
            row = cur.fetchone()
        if not row or not row.get("metadata"):
            return None
        try:
            return json.loads(row["metadata"])
        except json.JSONDecodeError:
            return None
```

- [ ] **Step 4: Run test — expect pass**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_mysql_client.py -v
```

Expected: 11 tests pass.

- [ ] **Step 5: Commit**

```bash
git add smoke-test/tests/zdu/framework/mysql_client.py \
        smoke-test/tests/zdu/framework/test_mysql_client.py
git commit -m "feat(zdu): MySQLClient — direct DB reads for state assertions"
```

---

## Task 5: Rewrite `ElasticsearchClient` with direct-ES methods

**Files:**

- Modify (rewrite): `smoke-test/tests/zdu/framework/es_client.py`
- Create: `smoke-test/tests/zdu/framework/test_es_client.py`

**Pattern:** Two-headed client — keeps GMS-mediated methods used by Suite A's `ValidationPhase`, adds direct-ES methods used by future ES-aware phases. Two `requests.Session` instances so token-bearing headers don't leak into ES calls.

- [ ] **Step 1: Identify existing callers of the old client (informational)**

```bash
grep -rn "ElasticsearchClient\|search_for_entity\|get_doc_count_for_entity_type" \
  smoke-test/tests/zdu/ | grep -v __pycache__
```

Confirm `search_for_entity` and `get_doc_count_for_entity_type` are the only methods called externally — these signatures must not change.

- [ ] **Step 2: Write the failing test**

```python
# smoke-test/tests/zdu/framework/test_es_client.py
"""Unit tests for ElasticsearchClient — uses mocked Sessions; never hits a real ES."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from tests.zdu.framework.es_client import ElasticsearchClient


def _resp(json_data: object, status: int = 200) -> MagicMock:
    r = MagicMock()
    r.status_code = status
    r.json.return_value = json_data
    r.raise_for_status = MagicMock()
    if status >= 400:
        r.raise_for_status.side_effect = Exception(f"HTTP {status}")
    return r


@pytest.fixture
def client() -> ElasticsearchClient:
    return ElasticsearchClient(gms_url="http://gms:8080", es_url="http://es:9200")


class TestListIndices:
    def test_filters_by_prefix(self, client: ElasticsearchClient) -> None:
        cat_payload = [
            {"index": "datasetindex_v2"},
            {"index": "datasetindex_v2_1714000000"},
            {"index": "dashboardindex_v2"},
        ]
        with patch.object(client._es_session, "get", return_value=_resp(cat_payload)):
            assert sorted(client.list_indices(prefix="datasetindex_v2")) == [
                "datasetindex_v2", "datasetindex_v2_1714000000",
            ]

    def test_empty_when_no_match(self, client: ElasticsearchClient) -> None:
        with patch.object(client._es_session, "get", return_value=_resp([])):
            assert client.list_indices(prefix="nope") == []


class TestGetAliasTargets:
    def test_returns_index_keys(self, client: ElasticsearchClient) -> None:
        payload = {"datasetindex_v2_1714000000": {"aliases": {"datasetindex_v2": {}}}}
        with patch.object(client._es_session, "get", return_value=_resp(payload)):
            assert client.get_alias_targets("datasetindex_v2") == ["datasetindex_v2_1714000000"]

    def test_404_returns_empty(self, client: ElasticsearchClient) -> None:
        with patch.object(client._es_session, "get", return_value=_resp({}, status=404)):
            assert client.get_alias_targets("missing") == []


class TestGetDocCount:
    def test_returns_count(self, client: ElasticsearchClient) -> None:
        with patch.object(client._es_session, "get", return_value=_resp({"count": 42})):
            assert client.get_doc_count("datasetindex_v2") == 42

    def test_404_returns_zero(self, client: ElasticsearchClient) -> None:
        with patch.object(client._es_session, "get", return_value=_resp({}, status=404)):
            assert client.get_doc_count("missing") == 0


class TestGetDoc:
    def test_returns_source(self, client: ElasticsearchClient) -> None:
        payload = {"_source": {"urn": "x"}, "_id": "abc"}
        with patch.object(client._es_session, "get", return_value=_resp(payload)):
            assert client.get_doc("datasetindex_v2", "abc") == {"urn": "x"}

    def test_404_returns_none(self, client: ElasticsearchClient) -> None:
        with patch.object(client._es_session, "get", return_value=_resp({}, status=404)):
            assert client.get_doc("datasetindex_v2", "abc") is None


class TestSearchForEntity:
    """GMS-mediated path retained for Suite A backwards compatibility."""

    def test_finds_urn_in_results(self, client: ElasticsearchClient) -> None:
        payload = {"value": {"entities": [{"entity": {"urn": "urn:li:dataset:x"}}]}}
        with patch.object(client._gms_session, "get", return_value=_resp(payload)):
            assert client.search_for_entity("urn:li:dataset:x") is True

    def test_missing_urn_returns_false(self, client: ElasticsearchClient) -> None:
        payload = {"value": {"entities": []}}
        with patch.object(client._gms_session, "get", return_value=_resp(payload)):
            assert client.search_for_entity("urn:li:dataset:x") is False
```

- [ ] **Step 3: Run test — expect failure**

Expected: most fail (rewritten signatures haven't landed).

- [ ] **Step 4: Implement the rewrite**

```python
# smoke-test/tests/zdu/framework/es_client.py
"""Two-headed Elasticsearch client.

- The ``gms_*`` methods speak to DataHub GMS (used by Suite A validation).
- The ``list_indices`` / ``get_alias_targets`` / ``get_doc_count`` /
  ``get_doc`` / ``get_mappings`` / ``list_tasks`` methods speak directly
  to Elasticsearch — required for ZDU assertions that must bypass the
  DataHub abstraction layer (alias swap, doc parity, reindex tasks).
"""

from __future__ import annotations

import logging
from typing import Any

import requests

log = logging.getLogger(__name__)

_HTTP_NOT_FOUND = 404


class ElasticsearchClient:
    def __init__(
        self,
        gms_url: str,
        es_url: str = "http://localhost:9200",
        token: str | None = None,
    ) -> None:
        self._gms_url = gms_url.rstrip("/")
        self._es_url = es_url.rstrip("/")
        self._gms_session = requests.Session()
        if token:
            self._gms_session.headers["Authorization"] = f"Bearer {token}"
        # ES has no auth in the dev compose; keep sessions separate so the
        # bearer token never leaks into direct-ES calls.
        self._es_session = requests.Session()

    # ── GMS-mediated ────────────────────────────────────────────────────────

    def search_for_entity(self, urn: str, entity_type: str = "DASHBOARD") -> bool:
        resp = self._gms_session.get(
            f"{self._gms_url}/entities/search",
            params={"q": "*", "type": entity_type.upper(), "count": "500"},
            timeout=30,
        )
        resp.raise_for_status()
        entities = resp.json().get("value", {}).get("entities", [])
        return any(e.get("entity", {}).get("urn") == urn for e in entities)

    def get_doc_count_for_entity_type(self, entity_type: str) -> int:
        resp = self._gms_session.get(
            f"{self._gms_url}/entities/search",
            params={"q": "*", "type": entity_type.upper(), "count": "0"},
            timeout=30,
        )
        resp.raise_for_status()
        return int(resp.json().get("value", {}).get("numEntities", 0))

    # ── Direct ES ───────────────────────────────────────────────────────────

    def list_indices(self, prefix: str) -> list[str]:
        resp = self._es_session.get(
            f"{self._es_url}/_cat/indices",
            params={"format": "json"},
            timeout=30,
        )
        resp.raise_for_status()
        return [r["index"] for r in resp.json() if r["index"].startswith(prefix)]

    def get_alias_targets(self, alias: str) -> list[str]:
        resp = self._es_session.get(f"{self._es_url}/_alias/{alias}", timeout=30)
        if resp.status_code == _HTTP_NOT_FOUND:
            return []
        resp.raise_for_status()
        return list(resp.json().keys())

    def get_doc_count(self, index_name: str) -> int:
        resp = self._es_session.get(f"{self._es_url}/{index_name}/_count", timeout=30)
        if resp.status_code == _HTTP_NOT_FOUND:
            return 0
        resp.raise_for_status()
        return int(resp.json().get("count", 0))

    def get_doc(self, index_name: str, doc_id: str) -> dict[str, Any] | None:
        resp = self._es_session.get(
            f"{self._es_url}/{index_name}/_doc/{doc_id}", timeout=30
        )
        if resp.status_code == _HTTP_NOT_FOUND:
            return None
        resp.raise_for_status()
        source = resp.json().get("_source")
        return source if isinstance(source, dict) else None

    def get_mappings(self, index_name: str) -> dict[str, Any]:
        resp = self._es_session.get(f"{self._es_url}/{index_name}/_mapping", timeout=30)
        resp.raise_for_status()
        body: dict[str, Any] = resp.json()
        first = next(iter(body.values()), {})
        mappings = first.get("mappings", {})
        return mappings if isinstance(mappings, dict) else {}

    def list_tasks(
        self, action_pattern: str = "indices:data/write/reindex"
    ) -> list[dict[str, Any]]:
        resp = self._es_session.get(
            f"{self._es_url}/_tasks",
            params={"actions": action_pattern, "detailed": "true"},
            timeout=30,
        )
        resp.raise_for_status()
        out: list[dict[str, Any]] = []
        for node in resp.json().get("nodes", {}).values():
            for task_id, task in node.get("tasks", {}).items():
                task["task_id"] = task_id
                out.append(task)
        return out
```

- [ ] **Step 5: Run test — expect pass**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_es_client.py -v
```

Expected: 9 tests pass.

- [ ] **Step 6: Commit**

```bash
git add smoke-test/tests/zdu/framework/es_client.py \
        smoke-test/tests/zdu/framework/test_es_client.py
git commit -m "feat(zdu): direct ElasticsearchClient methods (alias, count, doc, mappings, tasks)"
```

---

## Task 6: Add `scenario_type` + `suite` to `ZDUTestScenario`; refactor `ScenarioExecutor.validate(scenario, ctx)`

**Files:**

- Modify: `smoke-test/tests/zdu/framework/scenario_loader.py`
- Modify: `smoke-test/tests/zdu/framework/test_scenario_loader.py` (append cases)
- Modify: `smoke-test/tests/zdu/scenarios.csv` (header only; data rows untouched)
- Modify: `smoke-test/tests/zdu/framework/phases/validation.py` (drop now-redundant `seeded_by_tc` building)

**Why combined:** The signature change on `ScenarioExecutor.validate()` is what enables the registry in Task 7 to consume it without an adapter. Tying the field-additions and the signature change into one task makes the diff coherent.

- [ ] **Step 1: Append failing tests for new fields**

Append to `smoke-test/tests/zdu/framework/test_scenario_loader.py` (do not delete existing tests):

```python
from tests.zdu.framework.suite import Suite


def test_parse_row_default_scenario_type_is_aspect_migration():
    row = {
        "TC#": "1", "Test Category": "Single Hop Migration",
        "Name": "Full sweep single hop", "Description": "",
        "Prerequisite Steps": "", "Test Steps": "",
        "Expected Result": "", "Result": "Pass",
        "Details": "", "Scenario Type": "",
    }
    s = ScenarioLoader()._parse_row(row)
    assert s is not None
    assert s.scenario_type == "aspect_migration"
    assert s.suite == Suite.A


def test_parse_row_explicit_scenario_type():
    row = {
        "TC#": "101", "Test Category": "ES Phase 1",
        "Name": "Single-index reindex", "Description": "",
        "Prerequisite Steps": "", "Test Steps": "",
        "Expected Result": "", "Result": "",
        "Details": "", "Scenario Type": "es_phase1",
    }
    s = ScenarioLoader()._parse_row(row)
    assert s is not None
    assert s.scenario_type == "es_phase1"
    assert s.suite == Suite.B


def test_parse_row_unknown_tc_range_returns_none():
    row = {
        "TC#": "999", "Test Category": "x", "Name": "x", "Description": "",
        "Prerequisite Steps": "", "Test Steps": "", "Expected Result": "",
        "Result": "", "Details": "", "Scenario Type": "es_phase1",
    }
    assert ScenarioLoader()._parse_row(row) is None


# Test for the refactored validate(scenario, ctx) signature.
def test_executor_validate_takes_ctx_and_filters_by_tc():
    from unittest.mock import MagicMock
    from tests.zdu.framework.context import SeededEntity, TestContext
    from tests.zdu.framework.scenario_loader import ScenarioExecutor

    datahub = MagicMock()
    aspect_resp = MagicMock()
    aspect_resp.schema_version = 2
    datahub.get_aspect.return_value = aspect_resp

    ctx = TestContext()
    ctx.seeded_entities = [
        SeededEntity(
            urn="urn:li:dataset:tc1", aspect_name="globalTags",
            tc_number=1, seeded_data={}, expected_schema_version=2,
            validator=lambda d: True,
        ),
        SeededEntity(
            urn="urn:li:dataset:tc2", aspect_name="globalTags",
            tc_number=2, seeded_data={}, expected_schema_version=2,
            validator=lambda d: True,
        ),
    ]
    scenario = ScenarioLoader()._parse_row({
        "TC#": "1", "Test Category": "x", "Name": "x", "Description": "",
        "Prerequisite Steps": "", "Test Steps": "", "Expected Result": "",
        "Result": "", "Details": "", "Scenario Type": "",
    })
    assert scenario is not None

    result = ScenarioExecutor(datahub).validate(scenario, ctx)
    assert result.status == "PASS"
    # Only TC-1's URN was queried, not TC-2's
    datahub.get_aspect.assert_called_once_with("urn:li:dataset:tc1", "globalTags")
```

- [ ] **Step 2: Run test — expect failures**

Expected: `AttributeError: 'ZDUTestScenario' object has no attribute 'scenario_type'` and `TypeError: validate() missing 1 required positional argument`.

- [ ] **Step 3: Modify `ZDUTestScenario` and `_parse_row`**

In `smoke-test/tests/zdu/framework/scenario_loader.py`:

Add the import near the top:

```python
from .suite import Suite, suite_for_tc
```

Append to the `ZDUTestScenario` dataclass after `skip_reason`:

```python
    scenario_type: str = "aspect_migration"
    suite: Suite = Suite.A
```

Replace `_parse_row`:

```python
    def _parse_row(self, row: dict[str, str]) -> ZDUTestScenario | None:
        tc_str = row.get("TC#", "").strip()
        if not tc_str.isdigit():
            return None
        tc = int(tc_str)
        suite = suite_for_tc(tc)
        if suite is None:
            return None  # Unknown TC range — silently skip
        scenario_type = (row.get("Scenario Type") or "").strip() or "aspect_migration"
        # Suite A entries must have a known action — anything else is malformed
        if suite == Suite.A and tc not in _TC_ACTION:
            return None
        return ZDUTestScenario(
            tc_number=tc,
            category=row.get("Test Category", "").strip(),
            name=row.get("Name", "").strip(),
            description=row.get("Description", "").strip(),
            prerequisite_steps=row.get("Prerequisite Steps", "").strip(),
            test_steps=row.get("Test Steps", "").strip(),
            expected_result=row.get("Expected Result", "").strip(),
            current_status=row.get("Result", "").strip(),
            details=row.get("Details", "").strip(),
            starting_schema_version=_TC_STARTING_VERSION.get(tc),
            expected_schema_version=_TC_EXPECTED_VERSION.get(tc),
            action=_TC_ACTION.get(tc, ""),
            aspect_name=_TC_ASPECT_NAME.get(tc, "embed"),
            entity_type=_TC_ENTITY_TYPE.get(tc, "dashboard"),
            expected_to_fail=tc in KNOWN_FAILURES,
            skip_reason=KNOWN_FAILURES.get(tc),
            scenario_type=scenario_type,
            suite=suite,
        )
```

- [ ] **Step 4: Refactor `ScenarioExecutor.validate(scenario, ctx)`**

Replace `ScenarioExecutor.validate` to take `ctx` directly:

```python
    def validate(
        self, scenario: ZDUTestScenario, ctx: "TestContext"
    ) -> ValidationResult:
        # Build URN list from ctx — replaces the per-call argument the legacy
        # ValidationPhase used to compute on every scenario.
        urns = [
            e.urn for e in ctx.seeded_entities if e.tc_number == scenario.tc_number
        ]

        if scenario.tc_number == 23:
            return ValidationResult(
                tc_number=scenario.tc_number, name=scenario.name,
                status="SKIP", expected_to_fail=False,
                actual_result="Skipped — test steps not yet defined in sheet",
            )
        if scenario.expected_to_fail:
            return ValidationResult(
                tc_number=scenario.tc_number, name=scenario.name,
                status="XFAIL", expected_to_fail=True,
                actual_result="Expected failure — not reproduced in integration test",
                failure_reason=scenario.skip_reason,
            )
        if not urns:
            return ValidationResult(
                tc_number=scenario.tc_number, name=scenario.name,
                status="SKIP", expected_to_fail=False,
                actual_result="No URNs seeded for this scenario",
            )

        expected_version = scenario.expected_schema_version
        failures: list[str] = []
        for urn in urns:
            try:
                resp = self._datahub.get_aspect(urn, scenario.aspect_name)
                if (
                    expected_version is not None
                    and resp.schema_version != expected_version
                ):
                    failures.append(
                        f"{urn}: expected schemaVersion={expected_version}, "
                        f"got {resp.schema_version}"
                    )
            except Exception as exc:
                failures.append(f"{urn}: {exc}")

        if failures:
            return ValidationResult(
                tc_number=scenario.tc_number, name=scenario.name,
                status="FAIL", expected_to_fail=False,
                actual_result="; ".join(failures),
                failure_reason=failures[0],
            )
        return ValidationResult(
            tc_number=scenario.tc_number, name=scenario.name,
            status="PASS", expected_to_fail=False,
            actual_result=(
                f"All {len(urns)} URN(s) at expected schemaVersion={expected_version}"
            ),
        )
```

Add `from .context import TestContext` near the top (or use the string-quoted forward reference as shown).

- [ ] **Step 5: Update `ValidationPhase` for the new signature**

In `smoke-test/tests/zdu/framework/phases/validation.py`, in `run()`:

- Delete the `seeded_by_tc` dict building (lines ~33-37 in the existing file).
- Change `result = self._executor.validate(scenario, urns)` to `result = self._executor.validate(scenario, ctx)`.

- [ ] **Step 6: Add `Scenario Type` to the CSV header**

Read `smoke-test/tests/zdu/scenarios.csv` line 1 and append `,Scenario Type`. Existing data rows pick up `aspect_migration` via the default in `_parse_row`.

- [ ] **Step 7: Run all loader tests**

```bash
smoke-test/venv/bin/python -m pytest \
  smoke-test/tests/zdu/framework/test_scenario_loader.py \
  smoke-test/tests/zdu/framework/test_suite.py -v
```

Expected: all PASS, including new ones.

- [ ] **Step 8: Commit**

```bash
git add smoke-test/tests/zdu/framework/scenario_loader.py \
        smoke-test/tests/zdu/framework/phases/validation.py \
        smoke-test/tests/zdu/framework/test_scenario_loader.py \
        smoke-test/tests/zdu/scenarios.csv
git commit -m "refactor(zdu): scenario_type+suite fields; ScenarioExecutor.validate takes ctx"
```

---

## Task 7: `ScenarioTypeExecutor` Protocol + `ScenarioTypeRegistry`

**Files:**

- Create: `smoke-test/tests/zdu/framework/scenario_executor.py`
- Create: `smoke-test/tests/zdu/framework/test_scenario_executor.py`

**Pattern:** Strategy + registry. `ScenarioExecutor` (refactored in Task 6) already satisfies the protocol structurally, so it registers without an adapter.

- [ ] **Step 1: Write the failing test**

```python
# smoke-test/tests/zdu/framework/test_scenario_executor.py
"""Unit tests for ScenarioTypeRegistry."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

import pytest

from tests.zdu.framework.context import TestContext, ValidationResult
from tests.zdu.framework.scenario_executor import (
    ScenarioTypeExecutor, ScenarioTypeRegistry,
)
from tests.zdu.framework.scenario_loader import ZDUTestScenario
from tests.zdu.framework.suite import Suite


def _scenario(tc: int, stype: str) -> ZDUTestScenario:
    return ZDUTestScenario(
        tc_number=tc, category="x", name="x", description="",
        prerequisite_steps="", test_steps="", expected_result="",
        current_status="", details="",
        starting_schema_version=None, expected_schema_version=None,
        action="", aspect_name="embed", entity_type="dataset",
        expected_to_fail=False, skip_reason=None,
        scenario_type=stype, suite=Suite.A,
    )


@dataclass
class _FakeExecutor:
    label: str

    def validate(self, scenario: ZDUTestScenario, ctx: TestContext) -> ValidationResult:
        return ValidationResult(
            tc_number=scenario.tc_number, name=self.label,
            status="PASS", expected_to_fail=False,
            actual_result=f"validated by {self.label}",
        )


def test_register_and_dispatch():
    reg = ScenarioTypeRegistry()
    reg.register("aspect_migration", cast(ScenarioTypeExecutor, _FakeExecutor("am")))
    result = reg.validate(_scenario(1, "aspect_migration"), TestContext())
    assert result.status == "PASS"
    assert "am" in result.actual_result


def test_unknown_type_returns_skip():
    reg = ScenarioTypeRegistry()
    result = reg.validate(_scenario(1, "no_such_type"), TestContext())
    assert result.status == "SKIP"
    assert "no_such_type" in result.actual_result


def test_double_register_raises():
    reg = ScenarioTypeRegistry()
    reg.register("aspect_migration", cast(ScenarioTypeExecutor, _FakeExecutor("a")))
    with pytest.raises(ValueError, match="already registered"):
        reg.register("aspect_migration", cast(ScenarioTypeExecutor, _FakeExecutor("b")))
```

- [ ] **Step 2: Run test — expect failure**

Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement**

```python
# smoke-test/tests/zdu/framework/scenario_executor.py
"""Strategy pattern for per-scenario-type validation.

Each ``scenario_type`` (``aspect_migration``, ``es_phase1``, …) has one
executor implementing :class:`ScenarioTypeExecutor`. ValidationPhase
dispatches by asking the registry to validate a scenario; the registry
returns ``SKIP`` for unregistered types so a missing executor never
silently passes.
"""

from __future__ import annotations

from typing import Protocol

from .context import TestContext, ValidationResult
from .scenario_loader import ZDUTestScenario


class ScenarioTypeExecutor(Protocol):
    """Implemented by per-scenario-type validators (one per phase plan)."""

    def validate(
        self, scenario: ZDUTestScenario, ctx: TestContext
    ) -> ValidationResult: ...


class ScenarioTypeRegistry:
    """Maps ``scenario_type`` → :class:`ScenarioTypeExecutor`."""

    def __init__(self) -> None:
        self._executors: dict[str, ScenarioTypeExecutor] = {}

    def register(self, scenario_type: str, executor: ScenarioTypeExecutor) -> None:
        if scenario_type in self._executors:
            raise ValueError(
                f"Executor for scenario_type {scenario_type!r} already registered"
            )
        self._executors[scenario_type] = executor

    def validate(
        self, scenario: ZDUTestScenario, ctx: TestContext
    ) -> ValidationResult:
        executor = self._executors.get(scenario.scenario_type)
        if executor is None:
            return ValidationResult(
                tc_number=scenario.tc_number,
                name=scenario.name,
                status="SKIP",
                expected_to_fail=False,
                actual_result=(
                    f"No executor registered for scenario_type "
                    f"{scenario.scenario_type!r}"
                ),
            )
        return executor.validate(scenario, ctx)
```

- [ ] **Step 4: Run test — expect pass**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_scenario_executor.py -v
```

Expected: 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add smoke-test/tests/zdu/framework/scenario_executor.py \
        smoke-test/tests/zdu/framework/test_scenario_executor.py
git commit -m "feat(zdu): ScenarioTypeExecutor protocol + ScenarioTypeRegistry"
```

---

## Task 8: Extend `ZDUTestConfig` with new env-driven settings

**Files:**

- Modify: `smoke-test/tests/zdu/framework/config.py`

- [ ] **Step 1: Add fields**

Inside the `ZDUTestConfig` dataclass, add these alongside related existing fields:

```python
    # ── DataHub connection ───────────────────────────────────────────────────
    es_url: str = "http://localhost:9200"

    # ── MySQL connection ─────────────────────────────────────────────────────
    mysql_host: str = "localhost"
    mysql_port: int = 3306
    mysql_user: str = "datahub"
    mysql_password: str = "datahub"
    mysql_database: str = "datahub"

    # ── Test hooks (Java-side env vars, default 0 = no-op) ───────────────────
    reindex_delay_ms: int = 0
    catch_up_delay_ms: int = 0

    # ── Suite filter ─────────────────────────────────────────────────────────
    suites: list[str] = field(default_factory=list)
```

- [ ] **Step 2: Extend `from_env`**

```python
        if v := os.environ.get("ES_URL"):
            kwargs["es_url"] = v
        if v := os.environ.get("MYSQL_HOST"):
            kwargs["mysql_host"] = v
        if v := os.environ.get("MYSQL_PORT"):
            kwargs["mysql_port"] = int(v)
        if v := os.environ.get("ZDU_REINDEX_DELAY_MS"):
            kwargs["reindex_delay_ms"] = int(v)
        if v := os.environ.get("ZDU_CATCH_UP_DELAY_MS"):
            kwargs["catch_up_delay_ms"] = int(v)
        if v := os.environ.get("ZDU_SUITES"):
            kwargs["suites"] = [s.strip().lower() for s in v.split(",") if s.strip()]
```

- [ ] **Step 3: Smoke-import**

```bash
smoke-test/venv/bin/python -c "
from tests.zdu.framework.config import ZDUTestConfig
c = ZDUTestConfig.from_env()
print(c.es_url, c.mysql_host, c.suites)
"
```

Expected: prints defaults or env-overridden values.

- [ ] **Step 4: Commit**

```bash
git add smoke-test/tests/zdu/framework/config.py
git commit -m "feat(zdu): add es_url, mysql_*, *_delay_ms, suites to ZDUTestConfig"
```

---

## Task 9: Wire DI in `runner.py` (clients + registry, no adapter)

**Files:**

- Modify: `smoke-test/tests/zdu/framework/runner.py`

The runner now:

1. Constructs `ElasticsearchClient` with the new 3-arg form.
2. Constructs `MySQLClient` once.
3. Constructs a `ScenarioTypeRegistry` and registers the existing aspect-migration executor directly (no adapter — Task 6 made the signature compatible).
4. Applies the suite filter.

The phase pipeline is **not** changed yet — `ValidationPhase` still gets the legacy `ScenarioExecutor` constructor argument, which Task 10 swaps for the registry.

- [ ] **Step 1: Modify `__init__`**

```python
# In ZDUTestRunner.__init__, replace the client-construction block:

        from .es_client import ElasticsearchClient
        from .mysql_client import MySQLClient
        from .scenario_executor import ScenarioTypeRegistry

        self._docker = DockerComposeClient(
            config.project_dir, config.compose_files, config.compose_profiles
        )
        self._datahub = DataHubClient(config.gms_url, config.gms_token)
        self._es = ElasticsearchClient(
            gms_url=config.gms_url,
            es_url=config.es_url,
            token=config.gms_token,
        )
        self._mysql = MySQLClient(
            host=config.mysql_host, port=config.mysql_port,
            user=config.mysql_user, password=config.mysql_password,
            database=config.mysql_database,
        )
        # Strategy registry — phase plans add new executors here without
        # touching this constructor. ScenarioExecutor's validate(scenario, ctx)
        # signature satisfies ScenarioTypeExecutor structurally — no adapter.
        self._registry = ScenarioTypeRegistry()
        self._aspect_executor = ScenarioExecutor(self._datahub)
        self._registry.register("aspect_migration", self._aspect_executor)

        self._reporter = Reporter(config.report_path)
```

- [ ] **Step 2: Apply suite filter**

In `__init__`, after the existing `run_only_tc` / `skip_tc` filters:

```python
        if config.suites:
            allowed = set(config.suites)
            scenarios = [s for s in scenarios if s.suite.value in allowed]
```

- [ ] **Step 3: Smoke-test that the runner constructs**

```bash
smoke-test/venv/bin/python -c "
from tests.zdu.framework.config import ZDUTestConfig
from tests.zdu.framework.runner import ZDUTestRunner
r = ZDUTestRunner(ZDUTestConfig(), scenarios=[])
print('runner constructed:', type(r._registry).__name__)
print('  aspect_migration registered:', 'aspect_migration' in r._registry._executors)
"
```

Expected: prints OK.

- [ ] **Step 4: All unit tests still green**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add smoke-test/tests/zdu/framework/runner.py
git commit -m "refactor(zdu): runner injects MySQLClient + ScenarioTypeRegistry"
```

---

## Task 10: Route `ValidationPhase` through the registry

**Files:**

- Modify: `smoke-test/tests/zdu/framework/phases/validation.py`
- Modify: `smoke-test/tests/zdu/framework/runner.py` (constructor wiring)

This is the task that _proves_ Plan 0's strategy abstraction works — Suite A scenarios now dispatch through the registry, so any breakage shows up before Plan 1.

- [ ] **Step 1: Refactor `ValidationPhase` constructor + `run()`**

Replace the constructor signature:

```python
class ValidationPhase(Phase):
    name = "validation"

    def __init__(
        self,
        registry: "ScenarioTypeRegistry",
        scenarios: list["ZDUTestScenario"],
        es: "ElasticsearchClient | None" = None,
    ) -> None:
        self._registry = registry
        self._scenarios = scenarios
        self._es = es
```

Replace the `run()` method body (the dispatch loop and the post-loop write/read checks remain; only the per-scenario call changes):

```python
    def run(self, ctx: TestContext) -> PhaseResult:
        start = datetime.utcnow()
        t0 = time.monotonic()
        results: list[ValidationResult] = []

        for scenario in self._scenarios:
            result = self._registry.validate(scenario, ctx)
            results.append(result)
            log.info("TC-%03d [%s]: %s", scenario.tc_number, scenario.name, result.status)
            if result.status == "FAIL":
                log.warning("  FAIL reason: %s", result.failure_reason)

        # ... existing write_failures / regression checks unchanged ...
```

Add the import at the top:

```python
from ..scenario_executor import ScenarioTypeRegistry
```

Remove the now-unused import of `ScenarioExecutor`.

- [ ] **Step 2: Update `runner.py` to pass the registry**

Change the `("validation", ValidationPhase(...))` construction in the `phases` list:

```python
            ("validation", ValidationPhase(
                registry=self._registry,
                scenarios=self._scenarios,
                es=self._es,
            )),
```

- [ ] **Step 3: Run all unit tests**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ -v
```

Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
git add smoke-test/tests/zdu/framework/phases/validation.py \
        smoke-test/tests/zdu/framework/runner.py
git commit -m "refactor(zdu): ValidationPhase dispatches through ScenarioTypeRegistry"
```

---

## Task 11: Per-suite pytest markers + `--suite` CLI (with env merging)

**Files:**

- Modify: `smoke-test/tests/zdu/test_zdu_upgrade.py`
- Modify: `smoke-test/tests/zdu/__main__.py`
- Create (if needed): `smoke-test/tests/zdu/pytest.ini`

- [ ] **Step 1: Check whether marker registration exists**

```bash
find smoke-test -maxdepth 3 \( -name pytest.ini -o -name pyproject.toml -o -name setup.cfg \) \
  -exec grep -l "^\[pytest\]\|\[tool\.pytest\.ini_options\]" {} \;
```

If a registry exists, ADD the new markers there. Otherwise, create `smoke-test/tests/zdu/pytest.ini`:

```ini
[pytest]
markers =
    suite_a: Suite A — Aspect schema migration (TC-001..TC-023)
    suite_b: Suite B — ES Phase 1 incremental reindex (TC-101..TC-112)
    suite_c: Suite C — Rollback dual-write (TC-201..TC-208)
    suite_d: Suite D — ES Phase 2 catch-up (TC-301..TC-309)
    suite_e: Suite E — System-level sweep (TC-401..TC-408)
    suite_f: Suite F — Live traffic (TC-501..TC-507)
    suite_g: Suite G — Rollback (TC-601..TC-604)
    suite_h: Suite H — Failure recovery (TC-701..TC-705)
```

- [ ] **Step 2: Apply marker per scenario in `pytest_generate_tests`**

In `smoke-test/tests/zdu/test_zdu_upgrade.py`:

```python
def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "scenario" in metafunc.fixturenames:
        scenarios = (
            metafunc.config._zdu_scenarios
            if hasattr(metafunc.config, "_zdu_scenarios")
            else []
        )
        params = [
            pytest.param(
                s,
                marks=getattr(pytest.mark, f"suite_{s.suite.value}"),
                id=_scenario_id(s),
            )
            for s in scenarios
        ]
        metafunc.parametrize("scenario", params)
```

- [ ] **Step 3: Rewrite `__main__.py` to merge env + CLI**

In `smoke-test/tests/zdu/__main__.py`, replace the body of `main()`:

```python
def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    p = argparse.ArgumentParser(description="ZDU upgrade lifecycle integration test")
    p.add_argument("--gms-url", default=None)
    p.add_argument("--gms-token", default=None)
    p.add_argument("--skip", nargs="*", default=None, metavar="PHASE",
                   help="Phase names to skip (override ZDU_SKIP_PHASES env)")
    p.add_argument("--sweep-timeout", type=int, default=None)
    p.add_argument("--reader-workers", type=int, default=None)
    p.add_argument("--writer-workers", type=int, default=None)
    p.add_argument("--only-tc", nargs="*", type=int, default=None, metavar="TC_NUMBER")
    p.add_argument(
        "--suite", nargs="*", default=None,
        choices=["a", "b", "c", "d", "e", "f", "g", "h"], metavar="SUITE",
        help="Run only the listed suites (lowercase short codes)",
    )
    args = p.parse_args()

    # Start from env, then override with explicit CLI flags only.
    config = ZDUTestConfig.from_env()
    if args.gms_url is not None:
        config.gms_url = args.gms_url
    if args.gms_token is not None:
        config.gms_token = args.gms_token
    if args.skip is not None:
        config.skip_phases = args.skip
    if args.sweep_timeout is not None:
        config.sweep_timeout_s = args.sweep_timeout
    if args.reader_workers is not None:
        config.reader_workers = args.reader_workers
    if args.writer_workers is not None:
        config.writer_workers = args.writer_workers
    if args.only_tc is not None:
        config.run_only_tc = args.only_tc
    if args.suite is not None:
        config.suites = args.suite

    report = ZDUTestRunner(config).run()
    print(report.summary())
    sys.exit(0 if report.passed else 1)
```

- [ ] **Step 4: Verify Suite A still selectable, no warnings**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/test_zdu_upgrade.py \
  -m suite_a --collect-only -q 2>&1 | tail -15
```

Expected: 23 entries TC-001..TC-023; no `PytestUnknownMarkWarning`. If a warning appears, the `pytest.ini` is not being discovered — relocate to `smoke-test/pytest.ini` or add to `smoke-test/setup.cfg`.

- [ ] **Step 5: Verify the CLI help**

```bash
smoke-test/venv/bin/python -m tests.zdu --help 2>&1 | grep -A1 "\-\-suite"
```

Expected: `--suite` appears with the choices.

- [ ] **Step 6: Commit**

```bash
git add smoke-test/tests/zdu/test_zdu_upgrade.py \
        smoke-test/tests/zdu/__main__.py \
        smoke-test/tests/zdu/pytest.ini
git commit -m "feat(zdu): per-suite markers + --suite CLI; __main__ merges env + CLI"
```

---

## Task 12: Integration sanity check (Suite A end-to-end against live stack)

**Pre-requisite:** `scripts/dev/datahub-dev.sh status` reports a healthy local stack.

This task verifies that Foundation has not regressed Suite A. All 23 Suite-A scenarios must produce the same pass/xfail/skip outcomes as before. The new registry must be in the call path (look for "validated by aspect_migration" in logs / through assertion that registry contains the executor).

- [ ] **Step 1: Run Suite A end-to-end via pytest**

```bash
DATAHUB_GMS_URL=http://localhost:8080 \
DATAHUB_GMS_TOKEN="$(grep -E '^token: ' ~/.datahubenv | awk '{print $2}')" \
smoke-test/venv/bin/python -m pytest \
  smoke-test/tests/zdu/test_zdu_upgrade.py -m suite_a -v 2>&1 | tail -40
```

Expected: same overall outcome as before this plan — XFAIL count unchanged, no scenario that previously passed now fails.

- [ ] **Step 2: Verify the JSON report schema**

```bash
smoke-test/venv/bin/python -c "
import json
r = json.load(open('smoke-test/build/zdu-test-report.json'))
assert 'phases' in r and 'scenarios' in r and 'summary' in r
print('phases:', [p['name'] for p in r['phases']])
print('total scenarios:', r['summary']['total_scenarios'])
"
```

Expected: phases are `discovery seed upgrade sweep_and_io validation` (no rename — that's in future phase plans). Scenario count = 23.

- [ ] **Step 3: Spot-check `MySQLClient` against real data**

```bash
smoke-test/venv/bin/python -c "
from tests.zdu.framework.mysql_client import MySQLClient
c = MySQLClient()
print('embed by version:', c.count_aspects_by_schema_version('embed'))
"
```

Expected: prints a dict; keys are either `int` or `None`. **If you see a string key like `'null'` or `'2'`, the JSON_EXTRACT comment in `mysql_client.py` is wrong — file a follow-up to coerce the keys.**

- [ ] **Step 4: If anything regressed, fix and commit**

```bash
git add -p
git commit -m "fix(zdu): foundation regression discovered in integration check"
```

If nothing regressed, no commit is needed.

---

## Task 13: Code review of the entire plan's diff

**Files:** none modified — review only.

- [ ] **Step 1: Produce the diff**

```bash
git diff master...HEAD -- smoke-test/tests/zdu/ smoke-test/setup.py > /tmp/zdu-foundation.diff
wc -l /tmp/zdu-foundation.diff
```

- [ ] **Step 2: Dispatch the code reviewer**

Open a `feature-dev:code-reviewer` agent with:

> Review the diff at `/tmp/zdu-foundation.diff`. This PR lays the foundation for the ZDU E2E test framework — one new client (`MySQLClient`), a rewrite of `ElasticsearchClient` with direct-ES methods, a `ScenarioTypeRegistry` + `Protocol` strategy that already routes Suite A's existing executor, a `Suite` enum, and runner/config plumbing. **No phase implementation in this PR.**
>
> Check specifically:
>
> 1. Type hints complete; no unjustified `Any`.
> 2. DI applied correctly — phases inject clients, never construct them.
> 3. Strategy pattern: `ScenarioExecutor` registers structurally without an adapter (signature change to `(scenario, ctx)`).
> 4. Each new module has unit tests with mocked deps; no test requires a live stack.
> 5. JSON parsing is defensive (returns None / empty rather than raising).
> 6. Existing Suite A behaviour preserved — XFAIL set unchanged, scenario count unchanged.
> 7. `pymysql` connection lifecycle is correct (closed in `finally` via context manager).
> 8. No `print()` calls in framework code.
> 9. The `ValidationPhase` refactor (Task 10) does not silently change the post-loop `write_failures` and read-progression checks.
>
> Surface only HIGH-CONFIDENCE issues. Skip nits.

- [ ] **Step 3: Address review feedback**

Each finding becomes a small commit. Do not merge until all P1 findings are resolved.

- [ ] **Step 4: Final commit (if any)**

```bash
git commit -m "fix(zdu): code-review feedback on foundation"
```

---

## Self-Review

**Spec coverage** — this plan delivers Section 6.3 (file layout) partially:

- Created: `mysql_client.py` (Section 7.4), rewrote `es_client.py` (Section 7.3), `suite.py` (supports Section 10.1), `scenario_executor.py` (new — Strategy abstraction).
- Out of scope, deferred to phase plans: `phases/snapshot_t0.py` (Plan 1), `phases/upgrade_blocking.py` (Plan 2), and the rest. Phase-contract dataclasses move with their writers.

**Placeholder scan** — none. Every step contains exact code or commands. Task 5 explicitly notes the existing-caller signatures that must not change.

**Type / signature consistency** —

- `Suite` enum used identically across Tasks 3, 6, 7, 11.
- `ZDUTestScenario.scenario_type: str = "aspect_migration"` (Task 6) is what Task 7's registry indexes by.
- `ScenarioExecutor.validate(scenario: ZDUTestScenario, ctx: TestContext) -> ValidationResult` (Task 6) satisfies the `ScenarioTypeExecutor` protocol (Task 7) structurally — no adapter.
- `MySQLClient.__init__(host, port, user, password, database)` (Task 4) matches Task 9's instantiation.
- `ElasticsearchClient(gms_url=..., es_url=..., token=...)` (Task 5) matches Task 9.
- `ScenarioTypeRegistry.register(scenario_type, executor)` (Task 7) matches Task 9's `register("aspect_migration", self._aspect_executor)`.
- `ValidationPhase(registry=, scenarios=, es=)` (Task 10) matches Task 10's runner wiring.

**Risks called out:**

- Task 4 step 3 documents an integration dependency on `JSON_EXTRACT`'s return-typing. Task 12 step 3 proves it on real data.
- Task 11 step 4 documents the `pytest.ini` discovery dependency and the recovery action if discovery fails.
- Task 12 step 1 is the first task that requires a live stack. Tasks 1-11 are stack-independent.

---

## Execution Handoff

Plan complete and saved. Two execution options:

**1. Subagent-Driven (recommended)** — fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — execute tasks in this session using `superpowers:executing-plans`, batch with checkpoints.

Which approach?
