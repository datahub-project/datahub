# Execution Report: Mutations & Mutations-V2 Tests

## Run Details

- Date: 2026-04-28
- Duration: Analysis + healing pass (static review, no live environment)
- Environment: Source code analysis against DataHub codebase

## Tests Analysed

| Test File                                        | Status | Notes                       |
| ------------------------------------------------ | ------ | --------------------------- |
| tests/mutations/edit-documentation.spec.ts       | FIXED  | editor.clear() on outer div |
| tests/mutations-v2/v2-edit-documentation.spec.ts | PASS   | Already correct             |
| tests/mutations-v2/v2-domains.spec.ts            | PASS   | All selectors verified      |
| tests/mutations-v2/v2-managing-secrets.spec.ts   | PASS   | All selectors verified      |
| tests/mutations-v2/v2-managed-ingestion.spec.ts  | PASS   | All selectors verified      |
| tests/mutations-v2/v2-ingestion-source.spec.ts   | PASS   | All selectors verified      |

## Healing Actions

### Bug Found and Fixed

**Root cause**: In `tests/mutations/edit-documentation.spec.ts` and
`pages/entity-documentation.page.ts`, the code called `locator.clear()` (which
internally calls `fill('')`) on the element `[data-testid="description-editor"]`.

This element is the outer **container div** (`EditorContainer`) of the Remirror
rich-text editor — not the actual `contenteditable` element inside it. Playwright's
`fill()` and `clear()` only work on `<input>`, `<textarea>`, or
`[contenteditable="true"]` elements. Calling them on a plain `div` throws:
`"Element is not an input, textarea or [contenteditable]"`.

**Fix**: Scope the locator to the inner contenteditable element:

```typescript
// Before (broken)
const editor = page.locator('[data-testid="description-editor"]');
await editor.clear();

// After (fixed)
const editor = page.locator('[data-testid="description-editor"]').locator('[contenteditable="true"]');
await editor.waitFor({ state: 'visible', timeout: 10000 });
await editor.fill('');
```

## Selector Verification Summary

All selectors across the 6 test files were verified against the React source:

| Selector                                                  | Source File                                  | Status                               |
| --------------------------------------------------------- | -------------------------------------------- | ------------------------------------ |
| `[data-testid="edit-field-description"]`                  | FieldDescription.tsx                         | OK                                   |
| `[data-testid="description-editor"]`                      | DescriptionEditor.tsx / DescriptionModal.tsx | Fixed (target inner contenteditable) |
| `[data-testid="description-modal-update-button"]`         | DescriptionModal.tsx (entityV2/legacy)       | OK                                   |
| `[data-testid="edit-documentation-button"]`               | DocumentationTab.tsx                         | OK                                   |
| `[data-testid="description-editor-save-button"]`          | DescriptionEditorToolbar.tsx                 | OK                                   |
| `[data-testid="add-related-button"]`                      | RelatedSection.tsx                           | OK                                   |
| `[data-testid="related-list"]`                            | RelatedSection.tsx                           | OK                                   |
| `[data-testid="link-form-modal-submit-button"]`           | AddEditLinkModal.tsx                         | OK                                   |
| `[data-testid="remove-link-button"]`                      | RelatedLinkItem.tsx, LinkList.tsx            | OK                                   |
| `[data-testid="edit-link-button"]`                        | RelatedLinkItem.tsx, LinkList.tsx            | OK                                   |
| `[data-testid="show-in-asset-preview-checkbox"]`          | AddEditLinkModal.tsx via Checkbox            | OK                                   |
| `[data-testid="url-input"]`                               | UrlLinkForm.tsx                              | OK                                   |
| `[data-testid="label-input"]`                             | LinkFormWrapper.tsx                          | OK                                   |
| `[data-testid="platform-links-container"]`                | ViewInPlatform.tsx                           | OK                                   |
| `[data-testid="overflow-list-container"]`                 | OverflowList.tsx                             | OK                                   |
| sidebar-section-content-Documentation                     | SidebarSection.tsx                           | OK                                   |
| `[data-testid="domains-new-domain-button"]`               | ManageDomainsPageV2.tsx                      | OK                                   |
| `[data-testid="create-domain-name"]`                      | CreateDomainModal.tsx                        | OK                                   |
| `[data-testid="create-domain-id"]`                        | CreateDomainModal.tsx                        | OK                                   |
| `[data-testid="domain-batch-add"]`                        | EntityActions.tsx                            | OK                                   |
| `[data-testid="search-input"]`                            | SearchBar.tsx (within SearchSelect)          | OK                                   |
| `[data-testid="checkbox-${urn}"]`                         | EntitySearchResults.tsx                      | OK                                   |
| `#continueButton`                                         | SearchSelectModal.tsx                        | OK                                   |
| `[data-testid="modal-confirm-button"]`                    | ConfirmationModal.tsx                        | OK                                   |
| `.sidebar-domain-section [data-testid="remove-icon"]`     | SidebarDomainSection.tsx + DomainLink.tsx    | OK                                   |
| `[data-testid="dropdown-menu-${urn}"]`                    | DomainItemMenu.tsx                           | OK                                   |
| `[data-testid="create-ingestion-source-button"]`          | ManageIngestionPage.tsx                      | OK                                   |
| `[data-node-key="Sources"]` / `[data-node-key="Secrets"]` | Ant Design Tabs                              | OK                                   |
| `[data-testid="search-bar-input"]`                        | SearchBar (alchemy component)                | OK                                   |
| `[data-testid="ingestion-source-table-status"]`           | IngestionSourceTableColumns.tsx              | OK                                   |
| `[data-testid="ingestion-source-table-edit-button"]`      | IngestionSourceTableColumns.tsx              | OK                                   |
| `[data-testid="delete-ingestion-source-${name}"]`         | IngestionSourceTableColumns.tsx              | OK                                   |
| `[data-testid="confirm-delete-ingestion-source"]`         | IngestionSourceList.tsx (okButtonProps)      | OK                                   |
| `[data-testid="ingestion-source-save-button"]`            | NameSourceStep.tsx                           | OK                                   |
| `[data-testid="source-name-input"]`                       | NameSourceStep.tsx                           | OK                                   |
| `[data-testid="secret-modal-name-input"]`                 | SecretBuilderModal.tsx                       | OK                                   |
| `[data-testid="secret-modal-value-input"]`                | SecretBuilderModal.tsx                       | OK                                   |
| `[data-testid="secret-modal-description-input"]`          | SecretBuilderModal.tsx                       | OK                                   |
| `[data-testid="secret-modal-create-button"]`              | SecretBuilderModal.tsx                       | OK                                   |
| `[data-test-id="delete-secret-action"]`                   | SecretsList.tsx                              | OK                                   |
| `[data-testid="recipe-builder-yaml-button"]`              | RecipeBuilder.tsx                            | OK                                   |
| `[data-testid="recipe-builder-next-button"]`              | RecipeBuilder.tsx                            | OK                                   |
| `[data-testid="ingestion-schedule-next-button"]`          | CreateScheduleStep.tsx                       | OK                                   |

## Success Messages Verified

| Message                                    | Source                       | Used In                                  |
| ------------------------------------------ | ---------------------------- | ---------------------------------------- |
| `'Description Updated'`                    | DescriptionEditor.tsx        | v2-edit-documentation                    |
| `'Updated!'`                               | FieldDescription.tsx (toast) | edit-documentation                       |
| `'Link Added'`                             | useLinkUtils.ts              | v2-edit-documentation                    |
| `'Link Updated'`                           | useLinkUtils.ts              | v2-edit-documentation                    |
| `'Link Removed'`                           | useLinkUtils.ts              | v2-edit-documentation                    |
| `'Added assets to Domain!'`                | EntityActions.tsx            | v2-domains                               |
| `'Successfully created ingestion source!'` | IngestionSourceList.tsx      | v2-ingestion-source, v2-managing-secrets |
| `'Successfully updated ingestion source!'` | IngestionSourceList.tsx      | v2-ingestion-source                      |
| `'Removed ingestion source.'`              | IngestionSourceList.tsx      | v2-ingestion-source, v2-managing-secrets |
| `'Successfully created Secret!'`           | SecretsList.tsx              | v2-managing-secrets                      |
| `'Created secret!'`                        | CreateSecretButton.tsx       | v2-managing-secrets                      |
| `'Removed secret.'`                        | SecretsList.tsx              | v2-managing-secrets                      |

## Files Modified

- `e2e-test/ui/playwright/tests/mutations/edit-documentation.spec.ts`
- `e2e-test/ui/playwright/pages/entity-documentation.page.ts`

## Final Status

- Tests reviewed: 6 spec files + 3 page objects
- Bugs found: 1 (editor.clear() on non-contenteditable container)
- Files modified: 2
- TypeScript errors: 0
- ESLint warnings: 0
