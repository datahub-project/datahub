# Incidents V2 Test Documentation

## Overview

- **Feature**: Incidents V2 (Migrated from Cypress)
- **Test Suite**: `tests/incidents-v2/`
- **Page Objects**: `pages/incidents-page.ts`
- **Total Tests**: 5
- **Last Updated**: 2026-04-10

---

## Test Coverage Summary

### By Priority

- **P0 (Critical/Smoke)**: 2 tests
- **P1 (High/Functional)**: 3 tests

### By Type

- **Smoke**: 2 tests
- **Functional**: 3 tests

---

## Key Test Data Constants

```
EXISTING_INCIDENT_TITLE = 'test title'

NEW_INCIDENT_VALUES:
  NAME:        'Incident new name'
  DESCRIPTION: 'This is Description'
  TYPE:        'Freshness'
  PRIORITY:    'Critical'
  STAGE:       'Investigation'

EDITED_INCIDENT_VALUES:
  DESCRIPTION: 'Edited Description'
  PRIORITY:    'High'
  STAGE:       'In progress'
  TYPE:        'Freshness'
```

A shared timestamp is generated at describe scope so tests 2 and 3 operate on the same incident name (`Incident new name-<timestamp>`).

---

## Test Cases

### File: `tests/incidents-v2/v2-incidents.spec.ts`

Suite: `Incidents V2`

**Execution mode**: `serial` — tests 2 and 3 share state (create → update lifecycle).

**Preconditions (beforeEach)**:

- Feature flags intercepted via `apiMock.setFeatureFlags`:
  - `themeV2Enabled: true`
  - `themeV2Default: true`
  - `showNavBarRedesign: true`

---

#### Test 1: can view v1 incident

**Priority**: P0
**Type**: Smoke

**Objective**: Verify that a pre-existing v1 incident (`test title`) is visible in the Kafka dataset's incidents panel.

**Prerequisites**:

- Pre-seeded Kafka dataset `incidents-sample-dataset-v2` with an incident titled `test title`

**Steps**:

1. Navigate to the Kafka dataset incidents page
2. Locate the row with title `test title`

**Expected Results**:

- Incident row is visible and contains text `test title`

---

#### Test 2: create a v2 incident with all fields set

**Priority**: P0
**Type**: Smoke

**Objective**: Verify that a new v2 incident can be created with all fields (name, description, type, priority, stage, assignee) and appears in the incident list.

**Prerequisites**:

- Kafka dataset `incidents-sample-dataset-v2` accessible

**Steps**:

1. Navigate to Kafka dataset incidents page
2. Click "Create Incident" button
3. Fill incident name with `Incident new name-<timestamp>`
4. Fill description with `This is Description`
5. Select category `Freshness`
6. Select priority `Critical`
7. Select stage `Investigation`
8. Select the first available assignee
9. Dismiss open dropdowns
10. Submit the incident form
11. Expand `CRITICAL` priority group if collapsed

**Expected Results**:

- Incident row with `Incident new name-<timestamp>` is visible in the CRITICAL group
- Incident name badge is visible
- Stage shows `Investigation`
- Category shows `Freshness`
- "Resolve" button is visible and contains text `Resolve`

---

#### Test 3: can update incident & resolve incident

**Priority**: P1
**Type**: Functional

**Objective**: Verify that an existing incident (created in Test 2) can be edited (name, description, priority, stage, assignee, status) and resolved.

**Timeout**: 60 seconds (extended for full update + resolve cycle)

**Prerequisites**:

- Incident `Incident new name-<timestamp>` created by Test 2 (serial execution ensures ordering)

**Steps**:

1. Navigate to Kafka dataset incidents page
2. Expand `CRITICAL` group
3. Click on the row for `Incident new name-<timestamp>`
4. Click the edit icon
5. Clear name and fill with `Incident new name-<timestamp>-edited`
6. Clear description and fill with `Edited Description`
7. Select priority `High`
8. Select stage `In progress`
9. Select first assignee
10. Dismiss dropdowns
11. Select status `Resolved`
12. Submit the form
13. Filter by status `RESOLVED`
14. Scroll to and expand the `HIGH` priority group

**Expected Results**:

- Incident row `Incident new name-<timestamp>-edited` is visible in the HIGH group under RESOLVED
- Stage shows `In progress`
- Category shows `Freshness`
- Resolve button contains `Me` (indicating assigned to current user)

---

#### Test 4: Create V2 incident with all fields set and separate_siblings=false

**Priority**: P1
**Type**: Functional

**Objective**: Verify incident creation from a dataset with sibling datasets when `separate_siblings` is not forced to `true` (default behaviour — shows sibling-variant create button).

**Prerequisites**:

- BigQuery dataset `cypress_project.jaffle_shop.customers` with sibling datasets

**Steps**:

1. Navigate to BigQuery dataset incidents page (default `is_lineage_mode=false`)
2. Click the "Create Incident with Siblings" button
3. Select the first sibling from the dropdown
4. Verify drawer title contains `Create New Incident`
5. Fill incident name
6. Fill description (at index 1 for sibling mode's second editor)
7. Select category, priority, stage, assignee
8. Dismiss dropdowns and submit

**Expected Results**:

- `CRITICAL` group is visible and expandable
- Incident row with the new name exists in CRITICAL group
- Name badge visible, stage shows `Investigation`, category shows `Freshness`
- Resolve button visible with text `Resolve`

---

#### Test 5: Create V2 incident with all fields set and separate_siblings=true

**Priority**: P1
**Type**: Functional

**Objective**: Verify incident creation on a BigQuery dataset when `separate_siblings=true` is passed as a URL parameter (non-sibling create button variant).

**Steps**:

1. Navigate to BigQuery dataset incidents page with `separate_siblings=true`
2. Click the standard "Create Incident" button
3. Fill incident name with `Incident new name-<timestamp>-New`
4. Fill description, select category, priority, stage, assignee
5. Dismiss dropdowns and submit

**Expected Results**:

- `CRITICAL` group visible and expandable
- Incident row `Incident new name-<timestamp>-New` visible
- Name badge, stage `Investigation`, category `Freshness` visible
- Resolve button visible with text `Resolve`

---

## Page Objects Used

### IncidentsPage

**File**: `pages/incidents-page.ts`

**Key Methods**:

- `navigateToKafkaDatasetIncidents()` — navigate to the Kafka dataset incidents tab
- `navigateToBigQueryDatasetIncidents(extraParams?)` — navigate to BigQuery dataset incidents tab
- `getIncidentRow(title)` — locator for an incident row by title
- `getIncidentGroup(priority)` — locator for a priority group section
- `clickCreateIncidentBtn()` — click the standard "Create Incident" button
- `clickCreateIncidentBtnWithSiblings()` — click the sibling-variant create button
- `selectFirstSiblingFromDropdown()` — select first sibling dataset from the dropdown
- `expectDrawerTitleContains(text)` — assert drawer title contains given text
- `fillIncidentName(name)` — fill the name field
- `fillDescription(text, editorIndex?)` — fill the description editor (index 0 or 1 for sibling mode)
- `selectCategory(type)` — select incident category/type
- `selectPriority(priority)` — select priority level
- `selectStage(stage)` — select incident stage
- `selectFirstAssignee()` — select the first available assignee
- `dismissDropdowns()` — close any open dropdown panels
- `submitIncidentForm()` — submit the create/edit form
- `expandGroupIfNeeded(priority)` — expand a collapsed priority group
- `clickIncidentRow(title)` — open incident detail by clicking the row
- `clickEditIcon()` — enter edit mode
- `clearAndFillName(name)` — clear then fill the name field
- `clearAndFillDescription(text)` — clear then fill the description
- `selectStatus(status)` — set the incident status (e.g., `Resolved`)
- `filterByStatus(status)` — apply the status filter to the incidents list
- `expectIncidentRowExists(name)` — assert a row with given name is present
- `expectIncidentNameBadgeVisible(name)` — assert the name badge element is visible
- `expectIncidentRowStage(name, stage)` — assert the row shows the expected stage
- `expectIncidentRowCategory(name, category)` — assert the row shows the expected category
- `expectResolveButtonVisible(name)` — assert "Resolve" button is visible for the row
- `expectResolveButtonContains(name, text)` — assert resolve button text content
- `expectIncidentGroupVisible(priority)` — assert priority group is rendered

---

## Test Execution

### Run All Incident Tests

```bash
cd e2e-test/ui/playwright
npx playwright test tests/incidents-v2/
```

### Run a Specific Test

```bash
npx playwright test tests/incidents-v2/v2-incidents.spec.ts -g "can view v1 incident"
```

### Run Create/Update Lifecycle

```bash
npx playwright test tests/incidents-v2/v2-incidents.spec.ts -g "create|update"
```

---

## Dependencies

### Required Services

- DataHub backend with Theme V2 feature flags support
- `apiMock` fixture to intercept feature flag responses

### Fixtures Used

- `base-test` (from `fixtures/base-test.ts`) — authenticated context, `page`, `logger`, `logDir`, `apiMock`

### Pre-seeded Test Data

- Kafka dataset `incidents-sample-dataset-v2` with an existing incident titled `test title`
- BigQuery dataset `cypress_project.jaffle_shop.customers` with sibling datasets

---

## Known Issues / Limitations

- Tests 2 and 3 run in `serial` mode and share state — if Test 2 fails, Test 3 will also fail because the incident won't exist. Run them together, not in isolation.
- Test 3 has a 60-second timeout because the full update + resolve cycle involves multiple UI interactions.
- The shared `newIncidentName` variable uses `Date.now()` at module load time. In parallel workers this is safe, but in repeat runs within the same second the names could collide — the timestamp ensures uniqueness per test run, not per second.
- The sibling dataset tests (Tests 4 and 5) depend on a BigQuery dataset that must be pre-seeded separately from the Kafka dataset.

---

## Related Documentation

- Search Tests: `docs/test-suites/search-tests.md`
- Business Attributes Tests: `docs/test-suites/business-attributes-tests.md`
- Page Object: `pages/incidents-page.ts`

---

_Generated by The Scribe on 2026-04-10_
