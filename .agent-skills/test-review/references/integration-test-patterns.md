# Integration Test (Cypress) Patterns Reference

Code examples extracted from the DataHub Cypress integration test infrastructure at `smoke-test/tests/cypress/`.

---

## Cypress Launcher (Python pytest wrapper)

**Source:** `smoke-test/tests/cypress/integration_test.py`

The Python wrapper handles the full lifecycle: ingest test data, run Cypress, clean up.

### Data Ingestion

```python
# integration_test.py:144-173
def ingest_data(auth_session, graph_client):
    create_datahub_step_state_aspects(
        get_admin_username(), ONBOARDING_IDS,
        f"{CYPRESS_TEST_DATA_DIR}/{TEST_ONBOARDING_DATA_FILENAME}",
    )
    update_fixture_timestamps(CYPRESS_TEST_DATA_DIR)
    ingest_file_via_rest(auth_session, f"{CYPRESS_TEST_DATA_DIR}/{TEST_DATA_FILENAME}")
    ingest_file_via_rest(auth_session, f"{CYPRESS_TEST_DATA_DIR}/{TEST_DBT_DATA_FILENAME}")
    ingest_file_via_rest(auth_session, f"{CYPRESS_TEST_DATA_DIR}/{TEST_PATCH_DATA_FILENAME}")
    ingest_file_via_rest(auth_session, f"{CYPRESS_TEST_DATA_DIR}/{TEST_ONBOARDING_DATA_FILENAME}")
    ingest_file_via_rest(auth_session, f"{CYPRESS_TEST_DATA_DIR}/{TEST_INCIDENT_DATA_FILENAME}")
    ingest_time_lineage(graph_client)
```

### Fixture Teardown

```python
# integration_test.py:176-202
@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    ingest_data(auth_session, graph_client)
    yield
    delete_urns_from_file(graph_client, f"{CYPRESS_TEST_DATA_DIR}/{TEST_DATA_FILENAME}")
    delete_urns_from_file(graph_client, f"{CYPRESS_TEST_DATA_DIR}/{TEST_DBT_DATA_FILENAME}")
    delete_urns_from_file(graph_client, f"{CYPRESS_TEST_DATA_DIR}/{TEST_PATCH_DATA_FILENAME}")
    delete_urns_from_file(graph_client, f"{CYPRESS_TEST_DATA_DIR}/{TEST_ONBOARDING_DATA_FILENAME}")
    delete_urns(graph_client, get_time_lineage_urns())
    delete_urns_from_file(graph_client, f"{CYPRESS_TEST_DATA_DIR}/{TEST_INCIDENT_DATA_FILENAME}")
    # Also removes generated onboarding file from disk
    if os.path.exists(f"{CYPRESS_TEST_DATA_DIR}/{TEST_ONBOARDING_DATA_FILENAME}"):
        os.remove(f"{CYPRESS_TEST_DATA_DIR}/{TEST_ONBOARDING_DATA_FILENAME}")
```

---

## Cypress Batching

**Source:** `smoke-test/tests/cypress/integration_test.py:214-242`

```python
def _get_cypress_tests_batch():
    all_tests = _get_js_files("tests/cypress/cypress/e2e")

    with open("tests/cypress/test_weights.json") as f:
        weights_data = json.load(f)
    test_weights = {item["filePath"]: float(item["duration"][:-1]) for item in weights_data}

    tests_with_weights = []
    for test in all_tests:
        if test in test_weights:
            tests_with_weights.append((test, test_weights[test]))
        else:
            tests_with_weights.append(test)

    test_batches = bin_pack_tasks(tests_with_weights, env_vars.get_batch_count())
    return test_batches[env_vars.get_batch_number()]
```

Uses the same `bin_pack_tasks` from `conftest.py` but with Cypress-specific `test_weights.json`.

---

## Filtered Tests (Retry Mode)

**Source:** `smoke-test/tests/cypress/integration_test.py:245-269`

```python
def _get_filtered_or_batched_tests():
    filtered_tests_file = env_vars.get_filtered_tests_file()
    if filtered_tests_file:
        with open(filtered_tests_file) as f:
            tests = [line.strip() for line in f
                     if line.strip() and not line.strip().startswith("#")]
        return tests
    else:
        return _get_cypress_tests_batch()
```

---

## Cypress Execution

**Source:** `smoke-test/tests/cypress/integration_test.py:272-343`

```python
def test_run_cypress(auth_session):
    record_key = env_vars.get_cypress_record_key()
    # ...
    specs_str = ",".join([f"**/{f}" for f in _get_filtered_or_batched_tests()])
    test_spec_arg = f" --spec '{specs_str}' "

    command = f'{electron_args} NO_COLOR=1 NODE_OPTIONS="{node_options}" npx cypress run {record_arg} {test_spec_arg} {tag_arg}'
    proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            cwd=f"{CYPRESS_TEST_DATA_DIR}", text=True, bufsize=1)
    # Stream output via threads...
    return_code = proc.wait()
    assert return_code == 0
```

---

## Cypress Spec Patterns

### Unique Test Data (Isolation)

**Source:** `smoke-test/tests/cypress/cypress/e2e/mutations/domains.js:3-5`

```javascript
const test_domain_id = Math.floor(Math.random() * 100000);
const test_domain = `CypressDomainTest ${test_domain_id}`;
const test_domain_urn = `urn:li:domain:${test_domain_id}`;
```

Each `describe` block generates unique identifiers to avoid collisions with other test runs.

### GraphQL Interception

**Source:** `smoke-test/tests/cypress/cypress/e2e/mutations/domains.js:8-12`

```javascript
beforeEach(() => {
  cy.intercept("POST", "/api/v2/graphql", (req) => {
    aliasQuery(req, "appConfig");
  });
});
```

### Feature Flag Mocking

**Source:** `smoke-test/tests/cypress/cypress/e2e/mutations/domains.js:14-26`

```javascript
const setDomainsFeatureFlag = (isOn) => {
  cy.intercept("POST", "/api/v2/graphql", (req) => {
    if (hasOperationName(req, "appConfig")) {
      req.alias = "gqlappConfigQuery";
      req.on("response", (res) => {
        res.body.data.appConfig.featureFlags.nestedDomainsEnabled = isOn;
      });
    }
  });
};
```

### Test Structure

**Source:** `smoke-test/tests/cypress/cypress/e2e/mutations/domains.js:28-39`

```javascript
it("create domain", () => {
  setDomainsFeatureFlag(true);
  cy.login();
  cy.goToDomainList();
  cy.clickOptionWithText("New Domain");
  cy.waitTextVisible("Create New Domain");
  cy.get('[data-testid="create-domain-name"]').click().type(test_domain);
  cy.clickOptionWithText("Advanced");
  cy.get('[data-testid="create-domain-id"]').click().type(test_domain_id);
  cy.get('[data-testid="create-domain-button"]').click();
  cy.waitTextVisible(test_domain);
});
```

Key patterns:

- `cy.login()` -- custom command for authentication
- `cy.goToDomainList()` -- custom navigation command
- `cy.clickOptionWithText()` -- text-based interaction
- `cy.waitTextVisible()` -- assertion that handles async rendering
- `cy.get('[data-testid="..."]')` -- stable selector via test ID

---

## Cypress Custom Commands

Common custom commands used across specs:

| Command                        | Purpose                                       |
| ------------------------------ | --------------------------------------------- |
| `cy.login()`                   | Log in to DataHub                             |
| `cy.goToDomainList()`          | Navigate to domains page                      |
| `cy.clickOptionWithText(text)` | Click element containing text                 |
| `cy.waitTextVisible(text)`     | Wait for text to appear and assert visibility |
| `cy.clickOptionWithTestId(id)` | Click element by `data-testid` attribute      |

---

## Cypress Test Organization

**Source:** `smoke-test/tests/cypress/cypress/e2e/`

Tests are organized by feature area:

| Directory                      | Feature                                |
| ------------------------------ | -------------------------------------- |
| `mutations/`                   | CRUD operations (domains, tags, users) |
| `search/`, `searchV2/`         | Search functionality                   |
| `lineage/`, `lineageV2/`       | Lineage graph                          |
| `glossary/`, `glossaryV2/`     | Business glossary                      |
| `domains/`, `domainsV2/`       | Domain management                      |
| `containers/`, `containersV2/` | Container hierarchy                    |
| `login/`, `loginV2/`           | Authentication flows                   |
| `ingestionV2/`, `ingestionV3/` | Ingestion UI                           |
| `analytics/`                   | Analytics features                     |
| `ml/`                          | ML model features                      |

The `V2` suffix typically indicates tests for the newer UI version.
