# Search Tests

This directory contains Playwright end-to-end tests for DataHub's search functionality, converted from the original Cypress tests.

## Test Files

### 1. `search.spec.ts` - Core Search Functionality

Tests the fundamental search features:

- Basic wildcard search (`*`) and result validation
- Impossible query search (expecting 0 results)
- Direct URL navigation to filtered search results
- Dataset page navigation and metadata verification
- Glossary term facets and labels
- Advanced search with single filter (Column Glossary Term)
- Advanced search with AND logic (multiple filters)
- Advanced search with OR logic (any filter matching)

**Key Features Tested:**

- Search input and submission
- Result count validation
- Dataset detail page navigation
- Glossary term management
- Advanced search filters
- Filter combination logic (AND/OR)

### 2. `search-filters.spec.ts` - Search Filters V2

Tests the new search filters interface:

- Filter version detection (V1 vs V2)
- Adding and removing multiple filters
- Filter persistence in URL
- Active filter badges
- Clear all filters functionality
- Filter by entity type (Datasets)
- Filter by platform (Hive)
- Multiple simultaneous active filters

**Key Features Tested:**

- Search filters V2 interface
- More filters dropdown
- Filter search within modal
- Active filter management
- URL parameter handling
- Filter removal and clearing

### 3. `query-and-filter-search.spec.ts` - Combined Query and Filters

Tests search with various filter combinations:

- Filter by entity types (Dashboards, ML Models, Pipelines, Glossary Terms)
- Filter by platforms (Hive, HDFS, Airflow)
- Filter by tags through "More Filters"
- Combined filters (Type + Platform)
- Filter persistence across navigation
- Browser back button with filters

**Key Features Tested:**

- Entity type filtering
- Platform filtering
- Tag filtering via "More Filters"
- Filter combination
- Navigation state preservation
- Browser history integration

## Running the Tests

### Prerequisites

1. **DataHub must be running** on `http://localhost:9002` (or configured BASE_URL)
2. Test data must be ingested (Cypress test datasets)
3. Authentication credentials: `datahub` / `datahub` (or set TEST_USERNAME/TEST_PASSWORD env vars)

### Test Commands

Run all search tests:

```bash
cd e2e-test/ui/playwright
npm test tests/search/
```

Run specific test file:

```bash
npm test tests/search/search.spec.ts
npm test tests/search/search-filters.spec.ts
npm test tests/search/query-and-filter-search.spec.ts
```

Run in headed mode (see browser):

```bash
npm test tests/search/ -- --headed
```

Run in debug mode:

```bash
npm test tests/search/search.spec.ts -- --debug
```

Run with UI mode:

```bash
npm test tests/search/ -- --ui
```

## Test Data Requirements

These tests expect the following test data to be present in DataHub:

### Datasets

- `fct_cypress_users_created` - Hive dataset with:

  - Tag: `Cypress`
  - Column: `user_id` with description "Id of the user"
  - Description: "table containing all the users created on a single day"

- `cypress_logging_events` - Hive dataset

  - Can have glossary term `CypressTerm` added
  - Column with `CypressColumnInfo` glossary term
  - Description contains "log event"

- `SampleCypressHdfsDataset` - HDFS dataset

  - Column with `CypressColumnInfo` glossary term
  - Tag: `CypressFeatureTag`

- `fct_cypress_users_created_no_tag` - Dataset without tags

### Glossary Terms

- `CypressTerm` - General glossary term
- `CypressColumnInfo` - Column-level glossary term

### Tags

- `Cypress` - Test tag for datasets
- `CypressFeatureTag` - Feature tag for testing

### Other Entities

- Various Dashboards, ML Models, Pipelines for entity type filtering
- Airflow, Hive, HDFS platforms for platform filtering

## Page Object

The tests use the enhanced `SearchPage` class located at:
`e2e-test/ui/playwright/pages/SearchPage.ts`

### Key Methods

**Navigation:**

- `navigateToHome()` - Navigate to home page
- `navigate(path)` - Navigate to specific path

**Search Operations:**

- `search(query)` - Execute search with 2s wait
- `searchAndWait(query, waitTime)` - Search with custom wait time
- `clickResult(resultName)` - Click on specific result

**Filter Operations:**

- `selectFilterOption(filterName, optionLabel)` - Apply filter
- `selectFilterOptionThroughMoreFilters(filterName, optionLabel)` - Apply via More Filters
- `removeActiveFilter(filterLabel)` - Remove specific filter
- `clearAllFilters()` - Clear all active filters

**Advanced Search:**

- `clickAdvanced()` - Open advanced search
- `clickAddFilter()` - Add new filter
- `selectFilter(filterType)` - Select filter type
- `selectAdvancedFilter(filterName)` - Select advanced filter option
- `fillTextFilter(text)` - Fill text input filter
- `clickAllFilters()` / `clickAnyFilter()` - Toggle AND/OR logic

**Assertions:**

- `expectResultsCount(pattern)` - Verify result count matches pattern
- `expectNoResults()` - Verify 0 results
- `expectHasResults()` - Verify results exist
- `expectActiveFilter(filterLabel)` - Verify filter is active
- `expectUrlContains(urlPart)` - Verify URL contains parameter
- `expectTextVisible(text)` - Verify text is visible

## Test Patterns

### 1. Basic Search Test

```typescript
test("should search and verify results", async ({ searchPage }) => {
  await searchPage.searchAndWait("*", 5000);
  await searchPage.expectHasResults();
});
```

### 2. Filter Application Test

```typescript
test("should apply filter", async ({ searchPage }) => {
  await searchPage.searchAndWait("*", 2000);
  await searchPage.selectFilterOption("Type", "Datasets");
  await searchPage.expectActiveFilter("Datasets");
  await searchPage.expectUrlContains("filter__entityType");
});
```

### 3. Advanced Search Test

```typescript
test("should use advanced search", async ({ searchPage, page }) => {
  await searchPage.navigateToHome();
  await searchPage.searchAndWait("*", 2000);
  await searchPage.clickAdvanced();
  await searchPage.clickAddFilter();
  await searchPage.selectFilter("Column Glossary Term");

  const termOption = page.getByText("CypressColumnInfo");
  await termOption.click();

  await searchPage.expectTextVisible("SampleCypressHdfsDataset");
});
```

## Troubleshooting

### Server Not Running

**Error:** `Test timeout exceeded while waiting for locator`
**Solution:** Start DataHub server on `http://localhost:9002`

### Missing Test Data

**Error:** Tests fail with "element not found" or "0 results" when expecting data
**Solution:** Ingest Cypress test data using the smoke-test data ingestion scripts

### Authentication Failures

**Error:** Login page not appearing or credentials rejected
**Solution:**

- Verify credentials are correct (default: datahub/datahub)
- Check auth.setup.ts is configured correctly
- Ensure .auth/user.json is being generated

### Filter Not Found

**Error:** `locator click: Timeout` for filter elements
**Solution:**

- Verify search filters V2 is enabled in your DataHub instance
- Check that the filter name matches exactly (case-sensitive)
- Use `--headed` mode to visually debug

## Migration from Cypress

These tests were converted from Cypress tests located at:

- `smoke-test/tests/cypress/cypress/e2e/search/search.js`
- `smoke-test/tests/cypress/cypress/e2e/search/searchFilters.js`
- `smoke-test/tests/cypress/cypress/e2e/search/query_and_filter_search.js`

### Key Differences

1. **Authentication:** Playwright uses a setup file that runs once, Cypress logged in per test
2. **Waiting:** Playwright has built-in auto-waiting, reduced need for explicit waits
3. **Selectors:** Maintained same `data-testid` selectors for compatibility
4. **Assertions:** Playwright's `expect` with locators vs Cypress's chainable commands
5. **Page Objects:** Playwright uses proper POM pattern, Cypress used custom commands

## Contributing

When adding new search tests:

1. Add selectors to `SearchPage.ts` if needed
2. Follow existing test structure and naming
3. Use page object methods, not raw selectors in tests
4. Add proper TypeScript types
5. Document expected test data requirements
6. Run `npm test tests/search/` to verify all tests pass
