# ZDU E2E — Plan F-5: IO-Pool Direct-MySQL Seed (Closes Notion D5)

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Switch IO-pool seeding from the GMS REST API to direct-MySQL inserts so the IO-pool entities `urn:li:dashboard:(test,zdu-io-pool-{0..4})` always land at `schemaVersion=null` (v1) in `metadata_aspect_v2`, regardless of the running GMS's `ASPECT_MIGRATION_MUTATOR_ENABLED` flag. This makes the deterministic race-window assertion (TC-403) reproducible across single-image dev runs and two-image-tag CI runs alike — closing the Notion D5 review thread "Test mutator for race window".

**Architecture:** Two changes. (1) Add a new `MySQLClient.upsert_aspect_raw` method that performs an `INSERT … ON DUPLICATE KEY UPDATE` against `metadata_aspect_v2 (urn, aspect, version=0)`. (2) Replace `SeedPhase._seed_io_pool_via_api` with `_seed_io_pool_via_mysql` that calls the new method with `systemmetadata='{}'` (no `schemaVersion` key). The fix aligns the implementation with the design-doc spec at line 181: "5 dedicated IO-pool entities seeded **directly into MySQL** (bypassing GMS write-path mutators) so they always land at `schemaVersion=null` (v1)".

**Tech Stack:** Python 3 (existing), `pymysql` (existing). No new dependencies.

**Why this closes Notion D5:** The race window's correctness depends on the IO-pool being at v1 when `MigrateAspectsStep` runs — only then does the v1→v2 mutator fire, opening the 500ms `pre_write_delay_ms` window for concurrent writes. Today's API-based seed allows the GMS write-path mutators to pre-bump the IO-pool to v4 if `ASPECT_MIGRATION_MUTATOR_ENABLED=true` on the seed-time GMS. Direct-MySQL seeding sidesteps the mutator chain entirely, removing the environmental dependency.

**Out of scope (deferred):**

- Production-bean test mutator (Java) — not needed; the existing `EmbedV1ToV2Mutator` etc. fire on any v1 dashboard `embed` aspect, including the IO pool.
- Diagnosing why `ASPECT_MIGRATION_MUTATOR_ENABLED=true` doesn't propagate to the upgrade container in single-image dev runs — separate plan once the symptoms are reproducible after this change.
- Changing per-scenario seed paths — those continue to use the GMS API; the API path correctly applies write-path mutators for scenario entities (which may need to be at non-v1 starting versions per scenario).

**Code-review handoff:** Final task dispatches `feature-dev:code-reviewer`.

---

## File Structure

```
smoke-test/tests/zdu/framework/
├── mysql_client.py                              MODIFY — add upsert_aspect_raw method
├── test_mysql_client.py                         MODIFY (or create) — 3 new method tests
├── phases/
│   └── seed.py                                  MODIFY — replace _seed_io_pool_via_api → _seed_io_pool_via_mysql
├── test_seed.py                                 MODIFY (or create) — update IO-pool seed tests for the new path
└── README.md                                    MODIFY — confirm "directly into MySQL" wording
```

The change is purely additive on `MySQLClient` (no breaking changes to existing callers). `SeedPhase` constructor already injects a `MySQLClient` (used by `_seed_via_db` for scenario entities); the IO-pool branch reuses the same client.

---

## Task 1: Add `MySQLClient.upsert_aspect_raw`

**Files:**

- Modify: `smoke-test/tests/zdu/framework/mysql_client.py`
- Modify or create: `smoke-test/tests/zdu/framework/test_mysql_client.py`

**Pattern:** New public method that issues `INSERT … ON DUPLICATE KEY UPDATE` for the `(urn, aspect, version=0)` row in `metadata_aspect_v2`. Mirrors the column set used by `get_aspect_raw` (read symmetric).

### 1.1 — Write failing tests

- [ ] **Step 1: Append tests to `test_mysql_client.py`** (create the file if it doesn't exist)

If `smoke-test/tests/zdu/framework/test_mysql_client.py` exists, append the following block. If it doesn't, create it with the imports + helper + class:

```python
"""Unit tests for MySQLClient.upsert_aspect_raw."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from tests.zdu.framework.mysql_client import MySQLClient


@pytest.fixture
def client() -> MySQLClient:
    return MySQLClient(host="127.0.0.1", port=3306, user="u", password="p", database="d")


class TestUpsertAspectRaw:
    def test_inserts_row_with_default_systemmetadata(
        self, client: MySQLClient
    ) -> None:
        cursor = MagicMock()
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor
        with patch.object(MySQLClient, "_conn") as conn_ctx:
            conn_ctx.return_value.__enter__.return_value = conn
            client.upsert_aspect_raw(
                urn="urn:li:dashboard:(test,zdu-io-pool-0)",
                aspect="embed",
                metadata='{"renderUrl":"http://x"}',
            )
        # Verify the SQL statement structure and the bound parameters.
        cursor.execute.assert_called_once()
        sql, params = cursor.execute.call_args.args
        assert "INSERT INTO metadata_aspect_v2" in sql
        assert "ON DUPLICATE KEY UPDATE" in sql
        assert params[0] == "urn:li:dashboard:(test,zdu-io-pool-0)"
        assert params[1] == "embed"
        assert params[2] == 0  # version
        assert params[3] == '{"renderUrl":"http://x"}'
        assert params[4] == "{}"  # default systemmetadata
        # createdby fallback
        assert params[6] == "urn:li:corpuser:datahub"

    def test_explicit_systemmetadata_passed_through(
        self, client: MySQLClient
    ) -> None:
        cursor = MagicMock()
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor
        with patch.object(MySQLClient, "_conn") as conn_ctx:
            conn_ctx.return_value.__enter__.return_value = conn
            client.upsert_aspect_raw(
                urn="urn:li:x", aspect="embed", metadata="{}",
                systemmetadata='{"schemaVersion":4}',
                createdby="urn:li:corpuser:zdu-test",
            )
        params = cursor.execute.call_args.args[1]
        assert params[4] == '{"schemaVersion":4}'
        assert params[6] == "urn:li:corpuser:zdu-test"

    def test_commits_after_execute(self, client: MySQLClient) -> None:
        cursor = MagicMock()
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor
        with patch.object(MySQLClient, "_conn") as conn_ctx:
            conn_ctx.return_value.__enter__.return_value = conn
            client.upsert_aspect_raw(
                urn="urn:li:x", aspect="embed", metadata="{}",
            )
        # Connection commit must fire so the row is durable.
        conn.commit.assert_called_once()
```

- [ ] **Step 2: Run tests — expect failure** (`AttributeError: 'MySQLClient' object has no attribute 'upsert_aspect_raw'`)

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_mysql_client.py -v 2>&1 | tail -10
```

### 1.2 — Implement the method

- [ ] **Step 3: Add `upsert_aspect_raw` to `MySQLClient`**

In `<REPO_ROOT>/smoke-test/tests/zdu/framework/mysql_client.py`, add the following method to the `MySQLClient` class — insert AFTER `get_aspect_raw` (around line 86):

```python
    def upsert_aspect_raw(
        self,
        urn: str,
        aspect: str,
        metadata: str,
        systemmetadata: str = "{}",
        createdby: str = "urn:li:corpuser:datahub",
        version: int = 0,
    ) -> None:
        """Insert (or replace) a single ``metadata_aspect_v2`` row.

        Bypasses the GMS write-path entirely — no MutationHook chain fires.
        Used by the IO-pool seed path so entities land at ``schemaVersion=null``
        (v1) in MySQL regardless of ``ASPECT_MIGRATION_MUTATOR_ENABLED`` on
        the running GMS.

        ``createdon`` is set to ``UTC_TIMESTAMP(6)`` server-side. ``systemmetadata``
        defaults to ``"{}"`` (no ``schemaVersion`` key) which ``get_schema_version``
        and the production read path both treat as v1.
        """
        sql = (
            "INSERT INTO metadata_aspect_v2 "
            "(urn, aspect, version, metadata, systemmetadata, createdon, createdby) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE "
            "metadata=VALUES(metadata), systemmetadata=VALUES(systemmetadata), "
            "createdon=VALUES(createdon), createdby=VALUES(createdby)"
        )
        from datetime import datetime as _dt

        params = (
            urn,
            aspect,
            version,
            metadata,
            systemmetadata,
            _dt.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            createdby,
        )
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, params)
            c.commit()
```

- [ ] **Step 4: Run tests — expect pass**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_mysql_client.py -v 2>&1 | tail -10
```

Expected: 3 tests pass.

- [ ] **Step 5: Run all framework tests for no regression**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 179 pass (176 baseline + 3 new).

- [ ] **Step 6: Commit**

```bash
git add smoke-test/tests/zdu/framework/mysql_client.py \
        smoke-test/tests/zdu/framework/test_mysql_client.py
git commit -m "feat(zdu): MySQLClient.upsert_aspect_raw — direct-DB row write for IO-pool seed"
```

Re-stage if pre-commit hooks reformat.

---

## Task 2: Switch IO-pool seed to direct MySQL

**Files:**

- Modify: `smoke-test/tests/zdu/framework/phases/seed.py`
- Modify or create: `smoke-test/tests/zdu/framework/test_seed.py`

**Pattern:** Replace the GMS-API-based `_seed_io_pool_via_api` with `_seed_io_pool_via_mysql` that calls the new `MySQLClient.upsert_aspect_raw`. The IO-pool docstring is rewritten to remove the environmental-dependency caveat (the docstring fix from F-1 was a workaround; F-5 is the real fix).

### 2.1 — Write failing tests

- [ ] **Step 1: Append tests to `test_seed.py`** (create the file if it doesn't exist)

If the file exists, append the following test class. If not, create it with appropriate imports + scaffolding:

```python
# ---------- IO-pool direct-MySQL seed tests ----------

from unittest.mock import MagicMock

from tests.zdu.framework.context import TestContext
from tests.zdu.framework.phases.seed import SeedPhase


class TestSeedIOPoolViaMysql:
    def test_seeds_io_pool_directly_via_mysql_not_api(self) -> None:
        datahub = MagicMock()
        mysql = MagicMock()
        # SeedPhase constructor signature — match the existing one. If the
        # constructor takes additional args, set them via kwargs as needed.
        phase = SeedPhase(datahub=datahub, mysql=mysql, scenarios=[])
        ctx = TestContext()
        # Invoke ONLY the IO-pool seed path; main scenario seed is empty.
        phase._seed_io_pool_via_mysql(ctx)
        # 5 IO-pool URNs (zdu-io-pool-0..4)
        assert mysql.upsert_aspect_raw.call_count == 5
        # GMS API must NOT have been called for IO-pool seeding.
        assert datahub.ingest_mcp.call_count == 0
        # Every call uses systemmetadata='{}' (no schemaVersion key)
        for call in mysql.upsert_aspect_raw.call_args_list:
            kwargs = call.kwargs
            assert kwargs.get("systemmetadata", "{}") == "{}" or kwargs.get(
                "systemmetadata"
            ) is None
        # ctx.io_pool_entities populated with 5 SeededEntity rows
        assert len(ctx.io_pool_entities) == 5
        urns = {e.urn for e in ctx.io_pool_entities}
        assert urns == {
            f"urn:li:dashboard:(test,zdu-io-pool-{i})" for i in range(5)
        }

    def test_io_pool_seed_aspect_is_embed(self) -> None:
        datahub = MagicMock()
        mysql = MagicMock()
        phase = SeedPhase(datahub=datahub, mysql=mysql, scenarios=[])
        ctx = TestContext()
        phase._seed_io_pool_via_mysql(ctx)
        for call in mysql.upsert_aspect_raw.call_args_list:
            assert call.kwargs.get("aspect") == "embed" or "embed" in call.args
```

- [ ] **Step 2: Run tests — expect failure** (`AttributeError: 'SeedPhase' object has no attribute '_seed_io_pool_via_mysql'`)

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_seed.py -v 2>&1 | tail -10
```

If the test file imports `SeedPhase` with a constructor signature that doesn't match the current one, `Read` `seed.py` to see the exact signature and adjust the test accordingly. Do NOT change the production constructor.

### 2.2 — Implement the swap

- [ ] **Step 3: Replace the IO-pool seed call site in `seed.py`**

In `smoke-test/tests/zdu/framework/phases/seed.py`, find the line:

```python
                self._seed_io_pool_via_api(ctx)
```

Replace with:

```python
                self._seed_io_pool_via_mysql(ctx)
```

- [ ] **Step 4: Replace the helper method**

Replace the entire `_seed_io_pool_via_api` method body with a new `_seed_io_pool_via_mysql` method:

```python
    def _seed_io_pool_via_mysql(self, ctx: TestContext) -> None:
        """Seed the IO pool with direct-MySQL inserts.

        Bypasses the GMS write-path entirely: rows are written to
        ``metadata_aspect_v2`` with ``systemmetadata='{}'`` so they always
        land at ``schemaVersion=null`` (v1) in the DB regardless of
        ``ASPECT_MIGRATION_MUTATOR_ENABLED`` on the running GMS. This makes
        the deterministic race-window assertion (TC-403) reproducible across
        single-image and two-image-tag runs.

        See design doc §4 / Plan F-5 for the rationale (closes Notion D5).
        """
        for i in range(_IO_POOL_SIZE):
            pool_urn = f"urn:li:dashboard:(test,zdu-io-pool-{i})"
            self._mysql.upsert_aspect_raw(
                urn=pool_urn,
                aspect=_IO_POOL_ASPECT,
                metadata=json.dumps(_IO_POOL_OLD_DATA),
                systemmetadata="{}",
            )
            log.info(
                "IO-pool[%d]: direct-MySQL upsert → %s schemaVersion=null",
                i,
                pool_urn,
            )
            ctx.io_pool_entities.append(
                SeededEntity(
                    urn=pool_urn,
                    aspect_name=_IO_POOL_ASPECT,
                    tc_number=0,
                    seeded_data=_IO_POOL_OLD_DATA,
                    expected_schema_version=_IO_POOL_EXPECTED_VERSION,
                    validator=_noop_validator,
                )
            )
```

If `import json` isn't already at the top of `seed.py`, add it.

- [ ] **Step 5: Verify there are no remaining references to `_seed_io_pool_via_api`**

```bash
cd <REPO_ROOT>
grep -rn "_seed_io_pool_via_api" smoke-test/tests/zdu/
```

Expected: zero matches. If any remain in the test file or callers, update them to the new name.

- [ ] **Step 6: Run tests — expect pass**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_seed.py -v 2>&1 | tail -15
```

Expected: 2 new tests pass. Other existing seed tests should remain unaffected.

- [ ] **Step 7: Run all framework tests**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 181 pass (179 + 2 new).

- [ ] **Step 8: Commit**

```bash
git add smoke-test/tests/zdu/framework/phases/seed.py \
        smoke-test/tests/zdu/framework/test_seed.py
git commit -m "feat(zdu): IO-pool seed via direct MySQL — closes Notion D5 race-window dependency"
```

Re-stage if pre-commit hooks reformat.

---

## Task 3: README — confirm direct-MySQL seed wording

**Files:**

- Modify: `smoke-test/tests/zdu/README.md`

**Pattern:** The README's `## Phases` overview at line 181 already says "seeded directly into MySQL (bypassing GMS write-path mutators) so they always land at `schemaVersion=null` (v1) in the DB regardless of `ASPECT_MIGRATION_MUTATOR_ENABLED`". The implementation now actually matches this. Verify and update any stale phrases that hedge.

- [ ] **Step 1: grep for stale wording**

```bash
grep -nE "API|GMS write-path|write-path mutators fire|ingest_mcp" smoke-test/tests/zdu/README.md | grep -i io-pool
```

If any line implies the IO-pool goes through GMS, update it.

- [ ] **Step 2: Add a Plan F-5 note in the IO-during-sweep section**

Find `## How Concurrent I/O Works During Sweep` (around line 282 in README). Append at the end of the section:

```markdown
**Why direct-MySQL seeding matters (Plan F-5):** The race-window assertion requires the IO-pool entities to be at v1 when `MigrateAspectsStep` runs — only then do the registered v1→v2 mutators fire, opening the 500ms `pre_write_delay_ms` window. Direct-MySQL seeding sidesteps the GMS write-path mutator chain entirely, so the IO-pool always starts at v1 regardless of the seed-time GMS's `ASPECT_MIGRATION_MUTATOR_ENABLED` flag. This makes the race-window assertion reproducible across single-image dev runs and two-image-tag CI runs alike.
```

- [ ] **Step 3: Commit**

```bash
git add smoke-test/tests/zdu/README.md
git commit -m "docs(zdu): document direct-MySQL IO-pool seed (Plan F-5)"
```

Re-stage if pre-commit hooks reformat.

---

## Task 4: Live integration check

**Pre-requisite:** Compose stack up.

- [ ] **Step 1: Spy verifies the seed call routes through MySQL**

```bash
cd <REPO_ROOT>/smoke-test && venv/bin/python << 'PY'
"""Spy verifies SeedPhase IO-pool path now goes through MySQL, not GMS."""
from unittest.mock import MagicMock
from tests.zdu.framework.context import TestContext
from tests.zdu.framework.phases.seed import SeedPhase

datahub = MagicMock()
mysql = MagicMock()
phase = SeedPhase(datahub=datahub, mysql=mysql, scenarios=[])
ctx = TestContext()
phase._seed_io_pool_via_mysql(ctx)
print("upsert_aspect_raw calls:", mysql.upsert_aspect_raw.call_count)
print("ingest_mcp calls:", datahub.ingest_mcp.call_count)
assert mysql.upsert_aspect_raw.call_count == 5
assert datahub.ingest_mcp.call_count == 0
print("io_pool_entities:", len(ctx.io_pool_entities))
assert len(ctx.io_pool_entities) == 5
print("LIVE-WIRING OK")
PY
```

Expected: ends with `LIVE-WIRING OK`.

- [ ] **Step 2: Run full Suite A — verify IO pool lands at v1**

```bash
cd <REPO_ROOT>/smoke-test
TOKEN=$(grep '  token:' ~/.datahubenv | awk '{print $2}')
DATAHUB_GMS_URL=http://localhost:8080 \
DATAHUB_GMS_TOKEN="$TOKEN" \
ZDU_SKIP_PHASES=upgrade,upgrade_nonblocking,rolling_restart \
venv/bin/python -m tests.zdu --suite a 2>&1 | tail -35
```

Expected: same baseline 14 PASS / 7 XFAIL / 1 pre-existing TC-020 FAIL / 1 SKIP. Watch the seed phase log for `IO-pool[0]: direct-MySQL upsert → urn:li:dashboard:(test,zdu-io-pool-0) schemaVersion=null` lines (one per IO-pool entry).

- [ ] **Step 3: Verify the IO pool is at v1 in MySQL after seed**

```bash
docker compose -f <REPO_ROOT>/docker/profiles/docker-compose.yml exec -T mysql \
  mysql -udatahub -pdatahub datahub -e "
  SELECT urn, JSON_EXTRACT(systemmetadata, '\$.schemaVersion') AS schema_version
  FROM metadata_aspect_v2
  WHERE urn LIKE 'urn:li:dashboard:(test,zdu-io-pool-%)'
  ORDER BY urn;
" 2>&1 | tail -10
```

Expected: 5 rows, all with `schema_version` = `NULL` (no `schemaVersion` key in `systemmetadata`).

- [ ] **Step 4: Cleanup IO-pool URNs** (housekeeping)

```bash
docker compose -f <REPO_ROOT>/docker/profiles/docker-compose.yml exec -T mysql \
  mysql -udatahub -pdatahub datahub -e "
  DELETE FROM metadata_aspect_v2 WHERE urn LIKE 'urn:li:dashboard:(test,zdu-io-pool-%)';
" 2>&1 | tail -3
```

(IO pool is reseeded fresh on every run; cleanup is courtesy.)

- [ ] **Step 5: If anything regressed, fix and commit**

```bash
git add -p
git commit -m "fix(zdu): live-validation regression in Plan F-5"
```

If nothing regressed, no commit needed.

---

## Task 5: Code review

**Files:** none modified — review only.

- [ ] **Step 1: Generate the diff**

```bash
cd <REPO_ROOT>
/usr/bin/git diff 651f0f4b73..HEAD -- smoke-test/tests/zdu/ > /tmp/zdu-plan-f5.diff
wc -l /tmp/zdu-plan-f5.diff
```

(`651f0f4b73` is the last Plan 8 commit. Adjust if newer commits intervene.)

- [ ] **Step 2: Dispatch `feature-dev:code-reviewer`**

Send this prompt:

> Review the diff at `/tmp/zdu-plan-f5.diff`. This PR closes Notion review thread D5 by switching IO-pool seeding from the GMS REST API to direct-MySQL inserts. The change makes the deterministic race-window assertion (TC-403) reproducible across single-image dev and two-image-tag CI runs by guaranteeing IO-pool entities land at `schemaVersion=null` (v1) in `metadata_aspect_v2`, regardless of the seed-time GMS's `ASPECT_MIGRATION_MUTATOR_ENABLED` flag.
>
> Concretely:
>
> - `MySQLClient.upsert_aspect_raw(urn, aspect, metadata, systemmetadata="{}", createdby="urn:li:corpuser:datahub", version=0)` — issues `INSERT … ON DUPLICATE KEY UPDATE` against `metadata_aspect_v2 (urn, aspect, version)`. Defaults align with the IO-pool's needs: empty systemmetadata, system user, version 0.
> - `SeedPhase._seed_io_pool_via_mysql` replaces `_seed_io_pool_via_api`. Same 5-URN namespace (`zdu-io-pool-0..4`), same aspect (`embed`), same payload — only the write path changed.
> - 3 unit tests for `upsert_aspect_raw` (default systemmetadata, explicit override, commit fired). 2 unit tests for the seed switch (MySQL called 5×; GMS API called 0×).
> - README's "How Concurrent I/O Works During Sweep" section gets a Plan F-5 paragraph.
>
> Check specifically:
>
> 1. **SQL safety:** `upsert_aspect_raw` uses parameterised SQL (no concatenation). The `INSERT … ON DUPLICATE KEY UPDATE` clause matches `metadata_aspect_v2`'s actual schema (urn / aspect / version is the primary key).
> 2. **Commit semantics:** the `with self._conn() as c, c.cursor() as cur:` block calls `c.commit()` after `cur.execute()`. The `_conn()` contextmanager's `__exit__` should not implicitly commit — confirm by reading `_conn`. (If it does, the explicit commit is harmless but redundant.)
> 3. **Default systemmetadata `"{}"`:** an empty JSON object means no `schemaVersion` key, which `get_schema_version` and the production read path treat as v1 (`DEFAULT_SCHEMA_VERSION`). Confirm.
> 4. **`createdon` value:** uses Python `datetime.utcnow()` formatted with millisecond precision. The column type in `metadata_aspect_v2` is `DATETIME(6)`. Confirm the format string `"%Y-%m-%d %H:%M:%S.%f"[:-3]` produces a string MySQL accepts.
> 5. **Removal of `_seed_io_pool_via_api`:** confirm grep yields zero references in `smoke-test/tests/zdu/`. The corresponding `datahub.ingest_mcp` call on the IO-pool path is now gone — this means the seed phase no longer requires GMS to be healthy for the IO pool. The other (scenario-entity) seed path still uses the API.
> 6. **Test isolation:** the new tests mock `MySQLClient._conn` via `patch.object`. Confirm the patch correctly returns a context manager whose `__enter__` yields the mocked connection. The mocked cursor must support `.execute(sql, params)`.
> 7. **Pipeline preservation:** `ctx.io_pool_entities` continues to be populated with 5 `SeededEntity` rows. Subsequent phases (Phase 8 `upgrade_nonblocking`, Phase 9 `runtime_migration`, Phase 10 validation) consume this list — confirm the new code populates the same fields with the same values as the old.
> 8. **Type hints complete:** `upsert_aspect_raw` parameters and return type annotated. `_seed_io_pool_via_mysql` return type is `None`.
> 9. **YAGNI:** No retries on the upsert, no idempotency check before insert (the `ON DUPLICATE KEY UPDATE` handles re-runs). No batched inserts (5 rows is trivial).
> 10. **README consistency:** the existing line 181 already claims direct-MySQL seeding for the IO pool; the implementation now matches. The new paragraph in `## How Concurrent I/O Works During Sweep` explains _why_.
>
> Surface only HIGH-CONFIDENCE issues. Skip nits.

- [ ] **Step 3: Address review feedback**

Each finding becomes a small commit. Re-review until approved.

- [ ] **Step 4: Final commit (if any)**

```bash
git commit -m "fix(zdu): code-review feedback on Plan F-5"
```

---

## Self-Review

**Spec coverage** (against design doc line 181 + Notion D5):

| Requirement                                                                                   | Task                                                                                        |
| --------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| IO-pool seeded "directly into MySQL (bypassing GMS write-path mutators)"                      | Task 2 (`_seed_io_pool_via_mysql`)                                                          |
| IO-pool always lands at `schemaVersion=null` regardless of `ASPECT_MIGRATION_MUTATOR_ENABLED` | Task 1 (default `systemmetadata="{}"` ⇒ no `schemaVersion` key)                             |
| Race-window deterministic across single-image and two-image-tag runs                          | Task 1 + Task 2 (combined)                                                                  |
| TC-403 race-window line-by-line interleave proof                                              | OUT OF SCOPE — separate plan; this plan only ensures the prerequisite (IO pool at v1) holds |
| Java test mutator                                                                             | OUT OF SCOPE — not needed; existing v1→v2 production mutators suffice once IO pool is at v1 |

**Placeholder scan:** None.

**Type / signature consistency:**

- `MySQLClient.upsert_aspect_raw(urn, aspect, metadata, systemmetadata="{}", createdby="urn:li:corpuser:datahub", version=0) -> None` (Task 1) — invoked by Task 2's `_seed_io_pool_via_mysql`.
- `SeedPhase._seed_io_pool_via_mysql(self, ctx: TestContext) -> None` (Task 2) — invoked by `SeedPhase.run`.
- `SeededEntity` shape unchanged from F-1 — `ctx.io_pool_entities` consumers (Phase 8 IO harness, Phase 9 probes) read the same fields.

**Risks called out:**

1. **No-op when MySQL row already exists at non-default version.** If a prior test run wrote IO-pool URNs at v4 (e.g., a pre-F-5 run that went through GMS API mutators) and didn't clean up, the `ON DUPLICATE KEY UPDATE` correctly resets `metadata` and `systemmetadata` back to v1. So re-runs on a dirty DB are safe.
2. **`_IO_POOL_OLD_DATA` payload format.** The seed encodes the dict via `json.dumps`. The previous `ingest_mcp` call wraps the payload in a `proposal` envelope with `aspect.value` set to a JSON-string. Direct MySQL stores the raw aspect JSON in the `metadata` column. The two are subtly different shapes — the production read path expects the raw aspect JSON, which is what the new code stores. Confirm by reading one IO-pool row in MySQL after the live integration check (Task 4 Step 3).
3. **`SeedPhase` constructor.** The test prompt assumes `SeedPhase(datahub=..., mysql=..., scenarios=[])`. If the actual constructor differs, adjust the test (do NOT change the production constructor — other plans depend on it). Use `Read` on `seed.py` first.
4. **Plan 7's dev-stack hang.** Plan 7 noted the upgrade-job hangs at 601s when no mutators register. Plan F-5 itself does NOT solve this (the upgrade container's mutator chain still depends on `ASPECT_MIGRATION_MUTATOR_ENABLED` reaching it). Plan F-5 only ensures that _if_ the chain is non-empty, the IO-pool entities will be at v1 and therefore migration-eligible. A separate plan should address why the env var doesn't propagate.
5. **`createdon` precision.** Python `datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]` produces millisecond precision (`'2026-05-10 02:00:00.123'`). MySQL `DATETIME(6)` accepts microsecond precision; the value is implicitly padded. If the column is `DATETIME(3)` instead, it's already correct. No test row depends on sub-second precision.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-10-zdu-plan-f5-iopool-direct-mysql-seed.md`. Two execution options:

**1. Subagent-Driven (recommended)** — fresh subagent per task, two-stage review, fast iteration.

**2. Inline Execution** — execute tasks in this session using `superpowers:executing-plans`, batch with checkpoints.

Per session policy: defaulting to subagent-driven execution.
