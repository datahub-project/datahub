# Search Test Documentation

## Overview

- **Feature**: Search (Core, Filters V2, SearchV2, Query & Filter)
- **Test Suite**: `tests/search/`
- **Page Objects**: `pages/search-page.ts`
- **Total Tests**: 36
  - `search.spec.ts`: 7 tests
  - `search-filters.spec.ts`: 5 tests
  - `searchv2.spec.ts`: 14 tests
  - `query-and-filter-search.spec.ts`: 10 tests
- **Last Updated**: 2026-04-10

---

## Test Coverage Summary

### By Priority

- **P0 (Critical/Smoke)**: 5 tests
- **P1 (High/Functional)**: 22 tests
- **P2 (Medium/Edge Cases)**: 9 tests

### By Type

- **Smoke**: 5 tests
- **Functional**: 25 tests
- **Edge Cases**: 6 tests

---

## Test Cases

---

### File: `tests/search/search.spec.ts`

Suite: `Search Functionality`

**Preconditions (beforeEach)**:

- Authenticated context via `base-test` (`loginFixture` handles auth — no login step required)
- `SearchPage` instantiated with page, logger, logDir
- Navigate to home page

---

#### Test 1: should search all entities and see results

**Priority**: P0
**Type**: Smoke

**Objective**: Verify that a wildcard search returns at least one result.

**Steps**:

1. Search for `*` and wait 5 seconds

**Expected Results**:

- At least one search result is visible

---

#### Test 2: should search with impossible query and find 0 results

**Priority**: P1
**Type**: Functional

**Objective**: Verify that a nonsensical search query returns an empty results state.

**Steps**:

1. Search for `zzzzzzzzzzzzzqqqqqqqqqqqqqzzzzzzqzqzqzqzq` and wait 5 seconds

**Expected Results**:

- No results / empty state is displayed

---

#### Test 3: should search, find a result, and visit the dataset page

**Priority**: P0
**Type**: Smoke

**Objective**: Verify end-to-end search → dataset page navigation and key dataset metadata.

**Prerequisites**:

- Pre-seeded dataset `fct_playwright_users_created` (Hive platform)

**Steps**:

1. Search for `fct_playwright_users_created`
2. Click the first result
3. Wait for URL to contain `fct_playwright_users_created`
4. Wait for network idle

**Expected Results**:

- URL matches `.*dataset.*fct_playwright_users_created.*`
- Text `Hive`, `Dataset`, `fct_playwright_users_created`, `user_id`, and `Id of the user` are all visible

---

#### Test 4: should filter by glossary term

**Priority**: P1
**Type**: Functional

**Steps**:

1. Search `*` and wait 2 seconds
2. Select filter `Glossary-Term` → `PlaywrightTerm`

**Expected Results**:

- URL contains `filter_glossaryTerms`
- Active filter badge `PlaywrightTerm` is visible
- Results are present

---

#### Test 5: should search and filter by glossary term

**Priority**: P1
**Type**: Functional

**Steps**:

1. Search `*`, then apply `Glossary-Term: PlaywrightTerm`

**Expected Results**:

- URL contains `filter_glossaryTerms`
- Results are present

---

#### Test 6: should combine glossary term filter with text search

**Priority**: P1
**Type**: Functional

**Steps**:

1. Search `playwright`
2. Apply `Glossary-Term: PlaywrightTerm`

**Expected Results**:

- URL contains `filter_glossaryTerms`
- Results visible
- Active filter `PlaywrightTerm` badge visible

---

#### Test 7: should filter by multiple values using glossary terms

**Priority**: P1
**Type**: Functional

**Steps**:

1. Search `*`, apply `Glossary-Term: PlaywrightTerm`

**Expected Results**:

- URL contains `filter_glossaryTerms`
- Results visible
- Active filter `PlaywrightTerm` visible

---

### File: `tests/search/search-filters.spec.ts`

Suite: `Search Filters V2`

**Preconditions (beforeEach)**:

- Authenticated context via `base-test`
- `SearchPage` instantiated, navigate to home

---

#### Test 8: should show search filters v2 by default

**Priority**: P0
**Type**: Smoke

**Steps**:

1. Search `*`

**Expected Results**:

- Filters V2 UI is visible
- Filters V1 UI is NOT visible

---

#### Test 9: should add and remove multiple filters with no issues

**Priority**: P1
**Type**: Functional

**Objective**: Full add/remove/clear filter lifecycle.

**Steps**:

1. Search `*`
2. Apply `Type: Datasets` — verify URL contains `filter__entityType` and active badge `Datasets`
3. Apply `Platform: HDFS` — verify URL contains `filter_platform` and active badge `HDFS`
4. Verify both active badges coexist
5. Remove `Datasets` filter — verify URL no longer contains `filter__entityType` and badge gone
6. Verify `HDFS` badge still present
7. Clear all filters — verify URL no longer contains `filter_platform` and `HDFS` badge gone

**Expected Results**:

- Each step produces the expected URL state and active badge visibility

---

#### Test 10: should filter by type and verify results

**Priority**: P1
**Type**: Functional

**Steps**:

1. Search `*`, apply `Type: Datasets`

**Expected Results**:

- URL contains `filter__entityType`
- Active filter `Datasets` visible
- Pagination is visible (results returned)

---

#### Test 11: should filter by platform and verify results

**Priority**: P1
**Type**: Functional

**Steps**:

1. Search `*`, apply `Platform: Hive`

**Expected Results**:

- URL contains `filter_platform`
- Active filter `Hive` visible
- Pagination visible

---

#### Test 12: should handle multiple active filters correctly

**Priority**: P1
**Type**: Functional

**Steps**:

1. Search `*`
2. Apply `Type: Datasets` and `Platform: Hive`
3. Verify `Datasets`, `Hive`, and `DATASET` (entity type badge) all visible
4. Remove `Datasets`
5. Verify `Datasets` badge gone, `Hive` badge still present

**Expected Results**:

- Correct filter state after each add/remove operation

---

### File: `tests/search/searchv2.spec.ts`

Suite: `SearchV2 Features`

**Preconditions (beforeEach)**:

- Authenticated context via `base-test`
- `SearchPage` instantiated, navigate to home

---

#### Test 13: should display SearchV2 interface by default

**Priority**: P0
**Type**: Smoke

**Steps**:

1. Search `*`

**Expected Results**:

- V2 filter UI visible, V1 NOT visible

---

#### Test 14: should perform autocomplete search

**Priority**: P1
**Type**: Functional

**Steps**:

1. Type `playwright` into the search input
2. Wait 1 second

**Expected Results**:

- Autocomplete dropdown is visible

---

#### Test 15: should show no results message when search returns empty

**Priority**: P1
**Type**: Functional

**Steps**:

1. Search `zzznonexistentqueryyy123`

**Expected Results**:

- No-results / empty state visible

---

#### Test 16: should clear search input using clear button

**Priority**: P1
**Type**: Functional

**Steps**:

1. Click search input (focus)
2. Fill with `test query`
3. Click the clear (X) button

**Expected Results**:

- Clear button visible after fill
- Search input value is `''` after clear

---

#### Test 17: should navigate through filter dropdowns

**Priority**: P1
**Type**: Functional

**Steps**:

1. Search `*`
2. Click the `Type` filter dropdown (`[data-testid="filter-dropdown-Type"]`)

**Expected Results**:

- Filter dropdown menu is visible
- "Update Filters" button is visible

---

#### Test 18: should toggle filters using More Filters dropdown

**Priority**: P1
**Type**: Functional

**Steps**:

1. Search `*`
2. Click `More Filters` dropdown
3. Wait 500 ms

**Expected Results**:

- At least one element matching `[data-testid^="more-filter-"]` is visible

---

#### Test 19: should display active filters with correct test IDs

**Priority**: P2
**Type**: Edge Case

**Steps**:

1. Search `*`, apply `Type: Datasets`

**Expected Results**:

- `[data-testid="active-filter-_entityType␞typeNames"]` visible
- `[data-testid="active-filter-value-_entityType␞typeNames-DATASET"]` visible

---

#### Test 20: should remove individual filters using remove button

**Priority**: P1
**Type**: Functional

**Steps**:

1. Search `*`
2. Apply `Type: Datasets` and `Platform: Hive`
3. Click `[data-testid="remove-filter-_entityType␞typeNames"]`

**Expected Results**:

- `Datasets` active filter gone
- `Hive` active filter still present

---

#### Test 21: should clear all filters using Clear All button

**Priority**: P1
**Type**: Functional

**Steps**:

1. Search `*`, apply `Type: Datasets` and `Platform: Hive`
2. Click "Clear All"

**Expected Results**:

- Both `Datasets` and `Hive` active filters are gone

---

#### Test 22: should persist search query in URL

**Priority**: P1
**Type**: Functional

**Steps**:

1. Search `playwright`

**Expected Results**:

- URL contains `query=playwright`

---

#### Test 23: should expand and collapse filter facets

**Priority**: P2
**Type**: Edge Case

**Steps**:

1. Search `*`
2. If any `[data-testid^="expand-facet-"]` elements exist, click the first one and wait

**Expected Results**:

- No errors thrown during expand interaction (conditional test — no-op if no facets found)

---

#### Test 24: should display search results with pagination

**Priority**: P0
**Type**: Smoke

**Steps**:

1. Search `*`, wait 3 seconds

**Expected Results**:

- Results are present
- Pagination is visible
- Text matching `of [0-9]+ result` is visible

---

#### Test 25: should handle filter option selection with checkboxes

**Priority**: P1
**Type**: Functional

**Steps**:

1. Search `*`, apply `Type: Datasets`

**Expected Results**:

- Active filter `Datasets` badge visible
- URL contains `filter__entityType`

---

#### Test 26: should filter search within filter dropdown

**Priority**: P1
**Type**: Functional

**Steps**:

1. Search `*`, apply `Platform: Hive`

**Expected Results**:

- Active filter `Hive` badge visible

---

#### Test 27: should maintain search state when clicking entity result and navigating back

**Priority**: P2
**Type**: Edge Case

**Objective**: Verify browser back navigation restores both search query and active filters.

**Steps**:

1. Search `playwright`, apply `Type: Datasets`
2. Record current URL
3. Click an entity result
4. Wait for network idle
5. Click browser back
6. Wait for network idle

**Expected Results**:

- URL matches the pre-navigation search URL
- Active filter `Datasets` is still shown

---

### File: `tests/search/query-and-filter-search.spec.ts`

Suite: `Query and Filter Search`

**Preconditions (beforeEach)**:

- Authenticated context via `base-test`
- `SearchPage` instantiated, navigate to home

---

#### Test 28: should filter by type: Dashboards

**Priority**: P1
**Type**: Functional

**Steps**:

1. Search `*`, apply `Type: Dashboards`
2. Click first entity result

**Expected Results**:

- URL contains `filter__entityType`
- Text `Dashboard` is visible on the entity page

---

#### Test 29: should filter by type: ML Models

**Priority**: P1
**Type**: Functional

**Steps**:

1. Search `*`, apply `Type: ML Models`
2. Click first entity result

**Expected Results**:

- URL contains `filter__entityType`
- Text `ML Model` visible on entity page

---

#### Test 30: should filter by type: Pipelines

**Priority**: P1
**Type**: Functional

**Steps**:

1. Search `*`, apply `Type: Pipelines`
2. Click first entity result

**Expected Results**:

- URL contains `filter__entityType`
- Text `Pipeline` visible on entity page

---

#### Test 31: should filter by type: Glossary Terms

**Priority**: P1
**Type**: Functional

**Steps**:

1. Search `*`, apply `Type: Glossary Terms`
2. Click first entity result

**Expected Results**:

- URL contains `filter__entityType`
- Text `Glossary Term` visible on entity page

---

#### Test 32: should filter by platform: Hive

**Priority**: P1
**Type**: Functional

**Steps**:

1. Search `*`, apply `Platform: Hive`
2. Click first entity result

**Expected Results**:

- URL contains `filter_platform`
- Text `Hive` visible on entity page

---

#### Test 33: should filter by platform: HDFS

**Priority**: P1
**Type**: Functional

**Steps**:

1. Search `*`, apply `Platform: HDFS`
2. Click first entity result

**Expected Results**:

- URL contains `filter_platform`
- Text `HDFS` visible on entity page

---

#### Test 34: should filter by platform: Airflow

**Priority**: P1
**Type**: Functional

**Steps**:

1. Search `*`, apply `Platform: Airflow`
2. Click first entity result

**Expected Results**:

- URL contains `filter_platform`
- Text `Airflow` visible on entity page

---

#### Test 35: should filter by tag

**Priority**: P1
**Type**: Functional

**Prerequisites**:

- Pre-seeded entity tagged with `PlaywrightFeatureTag`

**Steps**:

1. Search `*`, apply `Tag: PlaywrightFeatureTag`
2. Click first entity result

**Expected Results**:

- URL contains `filter_tags`
- Text `Tags` visible on entity page
- `[data-testid="tag-PlaywrightFeatureTag"]` visible
- Text `PlaywrightFeatureTag` visible

---

#### Test 36: should combine multiple filters and verify results

**Priority**: P1
**Type**: Functional

**Steps**:

1. Search `*`
2. Apply `Type: Datasets` and `Platform: Hive`
3. Click first entity result

**Expected Results**:

- URL contains `filter__entityType` and `filter_platform`
- Both `Datasets` and `Hive` active filters visible before navigation
- Text `Dataset` and `Hive` visible on entity page

---

#### Test 37: should preserve filters when navigating back

**Priority**: P2
**Type**: Edge Case

**Steps**:

1. Search `*`, apply `Type: Datasets`
2. Click first entity result
3. Click browser back

**Expected Results**:

- Active filter `Datasets` is restored
- URL still contains `filter__entityType`

---

## Selectors Reference

| Element                       | Selector                                                            | Page Object                       | Purpose                       |
| ----------------------------- | ------------------------------------------------------------------- | --------------------------------- | ----------------------------- |
| Search input                  | (via `searchPage.searchInput`)                                      | `SearchPage.searchInput`          | Enter search query            |
| Search clear button           | (via `searchPage.clearButton`)                                      | `SearchPage.clearButton`          | Clear current query           |
| Autocomplete dropdown         | (via `searchPage.autocompleteDropdown`)                             | `SearchPage.autocompleteDropdown` | Autocomplete suggestions      |
| Type filter dropdown          | `[data-testid="filter-dropdown-Type"]`                              | Inline                            | Open Type filter              |
| Filter dropdown menu          | (via `searchPage.filterDropdownMenu`)                               | `SearchPage.filterDropdownMenu`   | Filter option list            |
| Update Filters button         | (via `searchPage.updateFiltersButton`)                              | `SearchPage.updateFiltersButton`  | Apply filter selection        |
| More Filters dropdown         | (via `searchPage.moreFiltersDropdown`)                              | `SearchPage.moreFiltersDropdown`  | Expand additional filters     |
| Active filter (Type)          | `[data-testid="active-filter-_entityType␞typeNames"]`               | Inline                            | Verify type filter active     |
| Active filter value (Dataset) | `[data-testid="active-filter-value-_entityType␞typeNames-DATASET"]` | Inline                            | Verify DATASET active         |
| Remove Type filter            | `[data-testid="remove-filter-_entityType␞typeNames"]`               | Inline                            | Remove entity type filter     |
| Tag element                   | `[data-testid="tag-PlaywrightFeatureTag"]`                          | Inline                            | Verify tag on entity page     |
| Expand facet                  | `[data-testid^="expand-facet-"]`                                    | Inline                            | Expand filter facet           |
| More filter option            | `[data-testid^="more-filter-"]`                                     | Inline                            | Filter option in More Filters |

---

## Page Objects Used

### SearchPage

**File**: `pages/search-page.ts`

**Key Properties**:

- `searchInput` — main search text field
- `clearButton` — X button to clear input
- `autocompleteDropdown` — autocomplete suggestions container
- `filterDropdownMenu` — dropdown for filter options
- `updateFiltersButton` — apply filter selection button
- `moreFiltersDropdown` — "More Filters" toggle
- `pagination` — pagination component

**Key Methods**:

- `navigateToHome()` — navigate to the home/search page
- `searchAndWait(query, ms)` — type query and wait specified ms for results
- `expectHasResults()` — assert results list is non-empty
- `expectNoResults()` — assert empty state shown
- `clickResult(name)` — click a result matching the given name
- `clickEntityResult()` — click the first entity result
- `selectFilterOption(filterName, optionName)` — open a filter dropdown and select an option
- `expectActiveFilter(label)` — assert an active filter badge is visible
- `expectActiveFilterNotVisible(label)` — assert an active filter badge is NOT visible
- `removeActiveFilter(label)` — remove an active filter badge
- `clearAllFilters()` — click "Clear All" to remove all active filters
- `expectUrlContains(fragment)` / `expectUrlNotContains(fragment)` — URL assertion helpers
- `expectTextVisible(text)` — assert text is visible on page
- `expectFiltersV2Visible()` / `expectFiltersV1NotVisible()` — V2 UI assertions
- `expectPaginationVisible()` — assert pagination is visible

---

## Test Execution

### Run All Search Tests

```bash
cd e2e-test/ui/playwright
npx playwright test tests/search/
```

### Run Individual Files

```bash
npx playwright test tests/search/search.spec.ts
npx playwright test tests/search/search-filters.spec.ts
npx playwright test tests/search/searchv2.spec.ts
npx playwright test tests/search/query-and-filter-search.spec.ts
```

### Run by Feature

```bash
# Glossary term filter tests
npx playwright test tests/search/ -g "glossary term"

# Filter lifecycle tests
npx playwright test tests/search/ -g "add and remove"

# URL persistence tests
npx playwright test tests/search/ -g "persist|URL"
```

---

## Dependencies

### Required Services

- DataHub backend with search and filter APIs enabled

### Fixtures Used

- `base-test` (from `fixtures/base-test.ts`) — authenticated context, `page`, `logger`, `logDir`

### Pre-seeded Test Data

- Dataset: `fct_playwright_users_created` (Hive platform, PROD env)
  - Field: `user_id` with description `Id of the user`
- Glossary term: `PlaywrightTerm`
- Tag: `PlaywrightFeatureTag` (applied to at least one entity)
- Entities of types: Dashboard, ML Model, Pipeline, Glossary Term
- Platforms: Hive, HDFS, Airflow

---

## Known Issues / Limitations

- Test 23 (`expand/collapse filter facets`) is conditional — it silently passes if no expandable facets are present on the page.
- Some tests use `page.waitForTimeout()` for autocomplete debounce delays; these may need tuning on slow CI.
- The `separate_siblings` URL parameter tests in the Incidents suite (not here) share naming similarity but are unrelated.
- Active filter test IDs use a non-ASCII separator character (`␞` U+241E) which must be preserved exactly in selectors.

---

## Related Documentation

- Incidents Tests: `docs/test-suites/incidents-tests.md`
- Business Attributes Tests: `docs/test-suites/business-attributes-tests.md`
- Page Object: `pages/search-page.ts`

---

_Generated by The Scribe on 2026-04-10_
