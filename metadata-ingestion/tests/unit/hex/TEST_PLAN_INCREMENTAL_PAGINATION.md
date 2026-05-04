# Test Plan: Incremental Pagination Optimization

Tests for the `LAST_EDITED_AT DESC` sort + early termination + checkpoint-seeding changes.
All tests use mock Hex API responses and mock DataHub state store.

---

## Test file: `test_incremental_pagination.py`

### Setup helpers needed
- `_make_hex_project(id, title, last_edited_at, type="PROJECT")` — builds a mock Hex API project response dict
- `_make_hex_page(projects, after_cursor=None)` — builds a mock paginated projects-list response
- `_make_incremental_checkpoint(last_ingested_at_ms)` — builds a `HexIncrementalCheckpointState`
- `_make_stale_checkpoint(urns)` — builds a `GenericCheckpointState` with a list of URNs
- `_make_source(config_overrides, incremental_checkpoint, stale_checkpoint)` — builds a `HexSource` with both checkpoints pre-loaded

---

## Scenarios

### 1. First run — full iteration, no early termination

**Given:** No incremental checkpoint, no stale entity removal checkpoint.  
**When:** `_populate_registries(last_ingested_at_ms=None)` is called.  
**Then:**
- `fetch_projects` is called with `stop_before_ms=None`
- All pages are fetched (no early stop)
- All projects land in `project_registry` as full (none in `_light_project_ids`)
- No checkpoint seeding attempted

---

### 2. Incremental run — some projects changed, early termination fires mid-page

**Given:**  
- Checkpoint with `last_ingested_at_ms = T`
- Page 1 contains: project A edited at `T+1h` (new), project B edited at `T-1h` (old)
- Page 2 exists but should NOT be fetched

**When:** `_populate_registries(last_ingested_at_ms=T)`  
**Then:**
- Project A is in `project_registry` as **full** (not in `_light_project_ids`)
- Page 2 is never requested (early termination on page 1 after seeing project B)
- Project B is NOT in `project_registry` from Hex listing (seeded from checkpoint instead — see scenario 3)

---

### 3. Incremental run — checkpoint-seeded projects added as light stubs

**Given:**
- Checkpoint with `last_ingested_at_ms = T`
- Hex listing (early-terminated) only returns project A
- Stale entity removal checkpoint contains URNs:
  - `urn:li:dashboard:(hex,project_B_id)` — project not returned by Hex
  - `urn:li:dashboard:(hex,project_A_id)` — already in registry from Hex listing
  - `urn:li:chart:(hex,component_id)` — component (not a project)
  - `urn:li:container:abc` — workspace container

**When:** `_populate_registries(last_ingested_at_ms=T)`  
**Then:**
- `project_registry["project_B_id"]` exists as a minimal stub (`title=""`)
- `"project_B_id"` is in `_light_project_ids`
- `project_registry["project_A_id"]` is the full object from Hex (not replaced by stub)
- `"component_id"` is NOT in `project_registry` (chart URN ignored)
- Container URN is ignored

---

### 4. Incremental run — no projects changed (all light)

**Given:**
- Checkpoint with `last_ingested_at_ms = T`
- Page 1 has one project edited at `T-2h` (immediately triggers early termination)
- Stale checkpoint has 100 project URNs

**When:** `_populate_registries(last_ingested_at_ms=T)`  
**Then:**
- Only 1 page fetched (early termination on first item)
- All 100 projects from checkpoint are in `project_registry` as light stubs
- `len(_light_project_ids) == 100`

---

### 5. Incremental run — all projects changed (no early termination)

**Given:**
- Checkpoint with `last_ingested_at_ms = T`
- All projects in all pages have `last_edited_at > T`

**When:** `_populate_registries(last_ingested_at_ms=T)`  
**Then:**
- All pages fetched (no early termination)
- All projects in `project_registry` as full
- `_light_project_ids` is empty (or only contains checkpoint-seeded ones not in Hex response)

---

### 6. Incremental run — no stale entity removal state available

**Given:**
- Incremental checkpoint exists (`last_ingested_at_ms = T`)
- `state_provider.get_last_checkpoint(STALE_ENTITY_REMOVAL_JOB_ID)` returns `None`
- Hex listing early-terminates normally

**When:** `_populate_registries(last_ingested_at_ms=T)`  
**Then:**
- No crash
- Only projects from Hex listing are in `project_registry`
- No checkpoint seeding attempted (graceful degradation)

---

### 7. Platform instance prefix stripping

**Given:**
- `source_config.platform_instance = "my_instance"`
- Stale checkpoint contains URN `urn:li:dashboard:(hex,my_instance.abc-123)`
- Project `abc-123` not in Hex listing (skipped by early termination)

**When:** `_populate_registries(last_ingested_at_ms=T)`  
**Then:**
- `project_registry["abc-123"]` exists as a light stub (prefix correctly stripped)
- Key is `"abc-123"`, not `"my_instance.abc-123"`

---

### 8. No platform instance — no prefix stripping needed

**Given:**
- `source_config.platform_instance = None`
- Stale checkpoint contains URN `urn:li:dashboard:(hex,abc-123)`

**When:** `_populate_registries(last_ingested_at_ms=T)`  
**Then:**
- `project_registry["abc-123"]` exists as a light stub

---

### 9. Run history still fires for checkpoint-seeded (light stub) projects

**Given:**
- Project `stub-project` seeded from checkpoint as light stub (title="")
- `hex_api.fetch_latest_run(stub-project)` returns a COMPLETED run with `start_time > last_ingested_at`

**When:** `_enrich_run_history()` is called  
**Then:**
- `project.latest_run` is populated on the stub
- `map_project_last_refreshed` is called and emits a `lastRefreshed` PATCH MCP

---

### 10. Sort params — `fetch_projects` uses `LAST_EDITED_AT DESC`

**Given:** Any config  
**When:** `fetch_projects()` is called  
**Then:**
- The HTTP GET request to `/projects` contains `sortBy=LAST_EDITED_AT` and `sortDirection=DESC`
- NOT `sortBy=CREATED_AT` or `sortDirection=ASC`

---

### 11. Pagination stops after early termination (params["after"] cleared)

**Given:**
- Page 1: [new_project, old_project] with `after` cursor pointing to page 2
- Early termination fires on `old_project`

**When:** Pagination loop evaluates `while params["after"] and not exhausted`  
**Then:**
- `params["after"]` is set to `None` by early termination
- Page 2 is never requested
- Exactly 1 page of HTTP calls made

---

## Mock strategy

```python
# Mock Hex API: patch session.get to return controlled pages
# Use side_effect list for multi-page scenarios

def mock_projects_page(projects, after_cursor=None):
    resp = MagicMock()
    resp.ok = True
    resp.status_code = 200
    resp.json.return_value = {
        "values": projects,
        "pagination": {"after": after_cursor, "before": None}
    }
    resp.raise_for_status = MagicMock()
    return resp

# Mock state provider: patch get_last_checkpoint to return controlled states
with patch.object(source.state_provider, "get_last_checkpoint") as mock_checkpoint:
    mock_checkpoint.side_effect = lambda job_id, state_class: {
        HEX_INCREMENTAL_JOB_ID: incremental_state,
        StaleEntityRemovalHandler.STALE_ENTITY_REMOVAL_SOURCE_JOB_ID: stale_state,
    }.get(job_id)
```
