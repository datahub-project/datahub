# Business Attributes Test Documentation

## Overview

- **Feature**: Business Attributes (CRUD, Inheritance, Related Entities, Field Operations, Data Types)
- **Test Suite**: `tests/business-attributes/`
- **Page Objects**: `pages/business-attribute-page.ts`, `pages/dataset-page.ts`
- **Helpers**: `helpers/graphql-helper.ts`
- **Total Tests**: 9
  - `business-attribute-navigation.spec.ts`: 2 tests
  - `check-feature-flag.spec.ts`: 1 test
  - `business-attribute.spec.ts`: 6 tests
- **Last Updated**: 2026-04-10

---

## Test Coverage Summary

### By Priority

- **P0 (Critical/Smoke)**: 2 tests
- **P1 (High/Functional)**: 5 tests
- **P2 (Medium/Edge Cases)**: 2 tests

### By Type

- **Smoke**: 2 tests
- **Functional**: 5 tests
- **Edge Cases**: 2 tests

---

## Important: Feature Flag Dependency

**All tests in this suite skip automatically if the Business Attribute feature flag is disabled.**

The flag is `businessAttributeEntityEnabled` in the DataHub `appConfig.featureFlags` GraphQL response.

To enable: set environment variable `BUSINESS_ATTRIBUTE_ENTITY_ENABLED=true` and restart DataHub.

---

## Key Test Data Constants

```
DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,Playwright_logging_events,PROD)'
FIELD_NAME  = 'event_name'

TEST_ATTRIBUTE   = 'PlaywrightBusinessAttribute'    (CRUD test)
TEST_ATTRIBUTE_2 = 'PlaywrightAttribute'            (Inheritance test)
TEST_ATTRIBUTE_3 = 'PlaywrightTestAttribute'        (Data type test)
```

---

## Test Cases

---

### File: `tests/business-attributes/check-feature-flag.spec.ts`

Suite: `Business Attribute Feature Flag`

---

#### Test 1: should check if business attribute feature is enabled

**Priority**: P2
**Type**: Edge Case (diagnostic)

**Objective**: Query the DataHub GraphQL API to verify the Business Attribute feature flag state. Provides a clear, skippable diagnostic for CI environments where the feature may be disabled.

**Steps**:

1. POST a GraphQL query to `/api/v2/graphql`: `{ appConfig { featureFlags { businessAttributeEntityEnabled } } }`
2. Parse the JSON response
3. Log the result
4. If disabled: skip with message `Business Attribute feature is not enabled`

**Expected Results**:

- `businessAttributeEntityEnabled` is `true`
- If `false`: test is skipped (not failed) with a warning logged

---

### File: `tests/business-attributes/business-attribute-navigation.spec.ts`

Suite: `Business Attribute Navigation`

---

#### Test 2: should navigate to business attributes page

**Priority**: P0
**Type**: Smoke

**Objective**: Verify navigation to the Business Attributes listing page works and the page loads correctly.

**Steps**:

1. Check feature flag — skip if disabled
2. Navigate to the Business Attributes page
3. Verify the page URL and title

**Expected Results**:

- User is on the Business Attributes page (URL check passes)
- Page title is visible (with up to 10 second timeout for slow loads)

---

#### Test 3: should display business attribute page header

**Priority**: P0
**Type**: Smoke

**Objective**: Verify the Business Attributes page renders its header/title element.

**Steps**:

1. Check feature flag — skip if disabled
2. Navigate to the Business Attributes page
3. Verify page title visible (default timeout)

**Expected Results**:

- Page title element is visible

---

### File: `tests/business-attributes/business-attribute.spec.ts`

Suite: `Business Attributes`

**Preconditions (beforeEach)**:

- `BusinessAttributePage`, `DatasetPage`, `GraphQLHelper` instantiated with page, logger, logDir

---

#### Test 4: should create, view, and delete a business attribute

**Priority**: P1
**Type**: Functional

**Suite**: Business Attribute CRUD Operations

**Objective**: Full lifecycle — create attribute, verify it appears, attach it to a dataset field, detach it, then delete it.

**Steps**:

1. Check feature flag — skip if disabled
2. Navigate to Business Attributes, click Create
3. Create attribute named `PlaywrightBusinessAttribute`
4. Navigate back to Business Attributes listing
5. Verify `PlaywrightBusinessAttribute` is visible in the list
6. Navigate to dataset `Playwright_logging_events`
7. Close any open modal
8. Add `PlaywrightBusinessAttribute` to field `event_name`
9. Remove `PlaywrightBusinessAttribute` from field `event_name`
10. Navigate back to Business Attributes
11. Select `PlaywrightBusinessAttribute`
12. Delete the attribute
13. Navigate back to Business Attributes listing

**Expected Results**:

- After creation: attribute visible in listing
- After adding to field: field shows the attribute
- After removing from field: field no longer shows the attribute
- After deletion: attribute NOT visible in listing

---

#### Test 5: should inherit tags and terms from business attribute to dataset field

**Priority**: P1
**Type**: Functional

**Suite**: Business Attribute Inheritance

**Objective**: Verify that tags and glossary terms associated with a business attribute are inherited by the dataset field when the attribute is applied.

**Prerequisites**:

- `PlaywrightAttribute` pre-seeded with tag `Playwright` and glossary term `PlaywrightTerm`
- Dataset `Playwright_logging_events` accessible

**Steps**:

1. Check feature flag — skip if disabled
2. Navigate to the dataset, close any modal
3. Click schema field `event_name`
4. Verify `Business Attribute` section is visible
5. Click "Add Attribute" button for `event_name`
6. Select `PlaywrightAttribute` in the modal

**Expected Results**:

- `PlaywrightAttribute` name is visible in the field panel
- Glossary term `PlaywrightTerm` is visible (inherited)
- Tag `Playwright` is visible (inherited)

---

#### Test 6: should view related entities for a business attribute

**Priority**: P1
**Type**: Functional

**Suite**: Business Attribute Related Entities

**Steps**:

1. Check feature flag — skip if disabled
2. Navigate to Business Attributes listing
3. Select `PlaywrightAttribute`
4. Click the "Related Entities" tab

**Expected Results**:

- Related entities section shows no results (or empty state)
- Count indicator matches `/of [0-9]+/`

---

#### Test 7: should search related entities by query

**Priority**: P2
**Type**: Edge Case

**Suite**: Business Attribute Related Entities

**Steps**:

1. Check feature flag — skip if disabled
2. Navigate directly to Business Attribute related entities URL for URN `urn:li:businessAttribute:37c81832-06e0-40b1-a682-858e1dd0d449`
3. Wait for network idle
4. Search related entities with query `event_n`

**Expected Results**:

- Count indicator shows `/of 1/`
- Text `event_name` is visible in the results

---

#### Test 8: should remove business attribute from dataset field

**Priority**: P1
**Type**: Functional

**Suite**: Business Attribute Field Operations

**Steps**:

1. Check feature flag — skip if disabled
2. Navigate to dataset, close modal
3. Click schema field `event_name`
4. Locate the business attribute section for the field
5. Remove `PlaywrightAttribute` from the section

**Expected Results**:

- `PlaywrightAttribute` is no longer displayed in the field's business attribute section

---

#### Test 9: should update the data type of a business attribute

**Priority**: P1
**Type**: Functional

**Suite**: Business Attribute Data Type

**Steps**:

1. Check feature flag — skip if disabled
2. Navigate to Business Attributes listing
3. Select `PlaywrightTestAttribute`
4. Update its data type to `STRING`

**Expected Results**:

- Text `STRING` is visible in the attribute detail view

---

## Page Objects Used

### BusinessAttributePage

**File**: `pages/business-attribute-page.ts`

**Key Methods**:

- `checkBusinessAttributeFeature(graphqlHelper)` — returns `true` if the feature flag is enabled
- `navigateToBusinessAttributes()` — navigate to `/business-attribute`
- `expectOnBusinessAttributePage()` — assert URL is the BA listing page
- `expectPageTitleVisible(timeout?)` — assert page title element is visible
- `clickCreateButton()` — open the create attribute dialog
- `createAttribute(name)` — fill and submit the create attribute form
- `selectAttribute(name)` — click on an attribute in the listing
- `deleteAttribute()` — trigger delete on the currently selected attribute
- `expectAttributeVisible(name)` — assert an attribute row is visible
- `expectAttributeNotVisible(name)` — assert an attribute row is NOT visible
- `clickAddAttributeButton(fieldName)` — click "Add Attribute" for a given field
- `selectAttributeInModal(name)` — select an attribute from the picker modal
- `clickRelatedEntitiesTab()` — click the Related Entities tab on an attribute page
- `expectNoRelatedEntities()` — assert empty related entities state
- `expectRelatedEntitiesCount(pattern)` — assert count text matches regex
- `searchRelatedEntities(query)` — search within related entities
- `getBusinessAttributeSection(fieldName)` — locator for the BA section of a field
- `removeAttributeFromSection(section, attributeName)` — remove attribute from a field section
- `updateDataType(type)` — update the data type of a business attribute
- `expectTextVisible(text)` — assert text is visible

### DatasetPage

**File**: `pages/dataset-page.ts`

**Key Methods**:

- `navigateToDataset(urn)` — navigate to a dataset detail page by URN
- `closeAnyOpenModal()` — dismiss any open modal (e.g., welcome modal)
- `addBusinessAttributeToField(fieldName, attributeName)` — add a BA to a schema field
- `removeBusinessAttributeFromField(fieldName, attributeName)` — remove a BA from a schema field
- `clickSchemaField(fieldName)` — click a schema field to open its side panel

### GraphQLHelper

**File**: `helpers/graphql-helper.ts`

**Key Methods**:

- Constructed from `page`: `new GraphQLHelper(page)`
- Used by `checkBusinessAttributeFeature()` to query feature flags

---

## Test Execution

### Run All Business Attribute Tests

```bash
cd e2e-test/ui/playwright
npx playwright test tests/business-attributes/
```

### Run Navigation Tests Only

```bash
npx playwright test tests/business-attributes/business-attribute-navigation.spec.ts
```

### Run CRUD Tests

```bash
npx playwright test tests/business-attributes/business-attribute.spec.ts -g "CRUD"
```

### Run Feature Flag Check

```bash
npx playwright test tests/business-attributes/check-feature-flag.spec.ts
```

---

## Dependencies

### Required Services

- DataHub backend with Business Attribute feature enabled (`BUSINESS_ATTRIBUTE_ENTITY_ENABLED=true`)
- GraphQL API accessible at `/api/v2/graphql`

### Fixtures Used

- `base-test` (from `fixtures/base-test.ts`) — authenticated context, `page`, `logger`, `logDir`

### Pre-seeded Test Data

- Business Attribute `PlaywrightAttribute` with associated tag `Playwright` and glossary term `PlaywrightTerm`
- Business Attribute `PlaywrightTestAttribute` (for data type update test)
- Business Attribute URN `urn:li:businessAttribute:37c81832-06e0-40b1-a682-858e1dd0d449` mapped to `event_name` field
- Hive dataset `Playwright_logging_events` (URN: `urn:li:dataset:(urn:li:dataPlatform:hive,Playwright_logging_events,PROD)`)
  - Schema field: `event_name`

---

## Known Issues / Limitations

- All tests in `business-attribute.spec.ts` skip when the feature flag is disabled — this is intentional and not a failure.
- Test 4 (CRUD) is a stateful test that creates then deletes `PlaywrightBusinessAttribute`. If the test fails midway and the attribute is not cleaned up, subsequent runs may find the attribute pre-existing. Manual cleanup may be required.
- Tests 5 and 8 operate on the same field (`event_name`) and the same attribute (`PlaywrightAttribute`). The correct execution order is: Test 5 (attach) → Test 8 (detach). Running them out of order is safe since they each explicitly set the desired state.
- Test 7 uses a hardcoded URN for the direct URL navigation — if this attribute is re-created with a different URN the test will break.

---

## Related Documentation

- Search Tests: `docs/test-suites/search-tests.md`
- Auth Tests: `docs/test-suites/auth-tests.md`
- Page Object: `pages/business-attribute-page.ts`
- Page Object: `pages/dataset-page.ts`

---

_Generated by The Scribe on 2026-04-10_
