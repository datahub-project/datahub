# Cypress Test Analysis — mutations & mutationsV2

**Files Analyzed**:

- `smoke-test/tests/cypress/cypress/e2e/mutations/add_users.js` (SKIPPED — v1_ui_removing)
- `smoke-test/tests/cypress/cypress/e2e/mutations/dataset_health.js`
- `smoke-test/tests/cypress/cypress/e2e/mutations/dataset_ownership.js` (SKIPPED — v1_ui_removing)
- `smoke-test/tests/cypress/cypress/e2e/mutations/deprecations.js` (SKIPPED — v1_ui_removing)
- `smoke-test/tests/cypress/cypress/e2e/mutations/edit_documentation.js`
- `smoke-test/tests/cypress/cypress/e2e/mutations/manage_ingestion_secret_privilege.js` (SKIPPED — v1_ui_removing)
- `smoke-test/tests/cypress/cypress/e2e/mutationsV2/v2_domains.js`
- `smoke-test/tests/cypress/cypress/e2e/mutationsV2/v2_edit_documentation.js`
- `smoke-test/tests/cypress/cypress/e2e/mutationsV2/v2_ingestion_source.js`
- `smoke-test/tests/cypress/cypress/e2e/mutationsV2/v2_managed_ingestion.js`
- `smoke-test/tests/cypress/cypress/e2e/mutationsV2/v2_managing_secrets.js`

**Analysis Date**: 2026-04-28

---

## Test Structure Summary

| Cypress File                         | Tests | Status          | Playwright File                                           |
| ------------------------------------ | ----- | --------------- | --------------------------------------------------------- |
| add_users.js                         | 3     | `describe.skip` | tests/mutations/add-users.spec.ts                         |
| dataset_health.js                    | 1     | Active          | tests/mutations/dataset-health.spec.ts                    |
| dataset_ownership.js                 | 9     | `describe.skip` | tests/mutations/dataset-ownership.spec.ts                 |
| deprecations.js                      | 1     | `describe.skip` | tests/mutations/deprecations.spec.ts                      |
| edit_documentation.js                | 1     | Active          | tests/mutations/edit-documentation.spec.ts                |
| manage_ingestion_secret_privilege.js | 3     | `describe.skip` | tests/mutations/manage-ingestion-secret-privilege.spec.ts |
| v2_domains.js                        | 4     | Active          | tests/mutations-v2/v2-domains.spec.ts                     |
| v2_edit_documentation.js             | 6     | Active          | tests/mutations-v2/v2-edit-documentation.spec.ts          |
| v2_ingestion_source.js               | 1     | Active          | tests/mutations-v2/v2-ingestion-source.spec.ts            |
| v2_managed_ingestion.js              | 1     | Active          | tests/mutations-v2/v2-managed-ingestion.spec.ts           |
| v2_managing_secrets.js               | 1     | Active          | tests/mutations-v2/v2-managing-secrets.spec.ts            |

---

## Custom Commands Used

| Command                                            | Playwright Equivalent                                                        |
| -------------------------------------------------- | ---------------------------------------------------------------------------- |
| `cy.login()`                                       | `loginFixture` (handles automatically via `.auth/` state file)               |
| `cy.logout()`                                      | Inline: click manage-account-menu → log-out-menu-item                        |
| `cy.logoutV2()`                                    | Inline: `nav-sidebar-sign-out` click                                         |
| `cy.goToDataset(urn, name)`                        | `page.goto('/dataset/'+urn)` + `expect(name).toBeVisible()`                  |
| `cy.goToDomainList()`                              | `page.goto('/domains')`                                                      |
| `cy.goToIngestionPage()`                           | `page.goto('/ingestion')`                                                    |
| `cy.waitTextVisible(text)`                         | `expect(page.getByText(text)).toBeVisible()`                                 |
| `cy.ensureTextNotPresent(text)`                    | `expect(page.getByText(text)).not.toBeVisible()`                             |
| `cy.clickOptionWithText(text)`                     | `page.getByText(text).click()`                                               |
| `cy.clickOptionWithTestId(id)`                     | `page.locator('[data-testid="id"]').first().click({force:true})`             |
| `cy.enterTextInTestId(id, text)`                   | `page.locator('[data-testid="id"]').fill(text)`                              |
| `cy.getWithTestId(id)`                             | `page.locator('[data-testid="id"]')`                                         |
| `cy.openEntityTab(tab)`                            | `page.locator('div[id$="tab"]:nth-child(1)').click()`                        |
| `cy.openThreeDotDropdown()`                        | `page.locator('[data-testid="entity-header-dropdown"]').click()`             |
| `cy.hideOnboardingTour()`                          | `page.keyboard.press('Control+ +Meta+ +h')`                                  |
| `cy.skipIntroducePage()`                           | `page.evaluate(() => localStorage.setItem('skipAcrylIntroducePage','true'))` |
| `cy.setFeatureFlags(fn)`                           | `apiMock.setFeatureFlags({flag: value})`                                     |
| `cy.removeDomainFromDataset(urn, name, domainUrn)` | `DomainsPage.removeDomainFromDataset(...)`                                   |

---

## Network Mocking Patterns

| Cypress                                                                                         | Playwright                                     |
| ----------------------------------------------------------------------------------------------- | ---------------------------------------------- |
| `cy.intercept("POST", "/api/v2/graphql", req => { if hasOperationName(req, "appConfig") ... })` | `apiMock.setFeatureFlags({ flagName: value })` |
| `cy.setFeatureFlags(res => { res.body.data.appConfig.featureFlags.X = Y })`                     | `apiMock.setFeatureFlags({ X: Y })`            |

---

## Selectors Overview

| Selector                                                | Used In                                                        |
| ------------------------------------------------------- | -------------------------------------------------------------- |
| `[data-testid="create-ingestion-source-button"]`        | v2_ingestion_source, v2_managing_secrets                       |
| `[data-testid="ingestion-source-table-status"]`         | v2_ingestion_source, v2_managing_secrets                       |
| `[data-testid="ingestion-source-save-button"]`          | v2_ingestion_source, v2_managing_secrets                       |
| `[data-testid="ingestion-schedule-next-button"]`        | v2_ingestion_source                                            |
| `[data-testid="recipe-builder-yaml-button"]`            | v2_ingestion_source                                            |
| `[data-testid="recipe-builder-next-button"]`            | v2_ingestion_source                                            |
| `[data-testid="source-name-input"]`                     | v2_ingestion_source, v2_managing_secrets, v2_managed_ingestion |
| `[data-testid="create-secret-button"]`                  | v2_managing_secrets                                            |
| `[data-testid="secret-modal-name-input"]`               | v2_managing_secrets                                            |
| `[data-testid="secret-modal-value-input"]`              | v2_managing_secrets                                            |
| `[data-testid="secret-modal-description-input"]`        | v2_managing_secrets                                            |
| `[data-testid="secret-modal-create-button"]`            | v2_managing_secrets                                            |
| `[data-testid="delete-secret-action"]`                  | v2_managing_secrets                                            |
| `[data-testid="domains-new-domain-button"]`             | v2_domains                                                     |
| `[data-testid="create-domain-name"]`                    | v2_domains                                                     |
| `[data-testid="create-domain-id"]`                      | v2_domains                                                     |
| `[data-testid="create-domain-button"]`                  | v2_domains                                                     |
| `[data-testid="domain-batch-add"]`                      | v2_domains                                                     |
| `[data-testid="dropdown-menu-{urn}"]`                   | v2_domains                                                     |
| `[data-testid="edit-documentation-button"]`             | v2_edit_documentation                                          |
| `[data-testid="description-editor-save-button"]`        | v2_edit_documentation                                          |
| `[data-testid="add-related-button"]`                    | v2_edit_documentation                                          |
| `[data-testid="related-list"]`                          | v2_edit_documentation                                          |
| `[data-testid="link-form-modal-submit-button"]`         | v2_edit_documentation                                          |
| `[data-testid="remove-link-button"]`                    | v2_edit_documentation                                          |
| `[data-testid="edit-link-button"]`                      | v2_edit_documentation                                          |
| `[data-testid="platform-links-container"]`              | v2_edit_documentation                                          |
| `[data-testid="overflow-list-container"]`               | v2_edit_documentation                                          |
| `[data-testid="sidebar-section-content-Documentation"]` | v2_edit_documentation                                          |
| `[data-testid="${urn}-health-icon"]`                    | dataset_health                                                 |
| `[data-testid="assertions-details"]`                    | dataset_health                                                 |
| `[data-testid="description-editor"]`                    | edit_documentation                                             |
| `[data-testid="edit-field-description"]`                | edit_documentation                                             |
| `[data-testid="description-modal-update-button"]`       | edit_documentation                                             |

---

## New Page Objects Created

| File                                 | Covers                                                                      |
| ------------------------------------ | --------------------------------------------------------------------------- |
| `pages/ingestion.page.ts`            | Sources tab, Secrets tab, Snowflake form, Monaco editor, secret/source CRUD |
| `pages/domains.page.ts`              | Domain creation, entity assignment, entity removal, domain deletion         |
| `pages/entity-documentation.page.ts` | Documentation tab editing, link CRUD, v1 field description editing          |

---

## Migration Complexity: High

- 11 Cypress files → 11 Playwright spec files
- 4 files were `describe.skip` (V1 UI removal) → preserved as `test.describe.skip`
- 3 new page objects created
- Feature flag mocking via `apiMock.setFeatureFlags` throughout
- Serial test ordering required for domain lifecycle and edit-documentation tests
- Monaco editor special handling for managed ingestion
