# Execution Report: Mutations & Mutations-V2 Suites

## Run Details

- Date: 2026-04-28
- Suites analyzed: 7 test files
- Environment: Local DataHub stack

## Test Files Analyzed

| File                                             | Notes                               |
| ------------------------------------------------ | ----------------------------------- |
| tests/mutations/dataset-health.spec.ts           | Selectors verified OK               |
| tests/mutations/edit-documentation.spec.ts       | Selectors verified OK               |
| tests/mutations-v2/v2-domains.spec.ts            | **Fixed** — domain removal selector |
| tests/mutations-v2/v2-edit-documentation.spec.ts | Selectors verified OK               |
| tests/mutations-v2/v2-ingestion-source.spec.ts   | Selectors verified OK               |
| tests/mutations-v2/v2-managed-ingestion.spec.ts  | Selectors verified OK               |
| tests/mutations-v2/v2-managing-secrets.spec.ts   | Selectors verified OK               |

## Healing Actions

### Fix 1: `pages/domains.page.ts` — `removeDomainFromDataset`

**Root cause:** The domain sidebar close button selector changed.

Previously, the domain tag in `SidebarDomainSection` was a plain anchor `<a href="/domain/${urn}">` with `.anticon-close` inside. The component was refactored to use `DomainLink` (from `app/sharedV2/tags/DomainLink.tsx`) which renders a `[data-testid="remove-icon"]` close button and uses `ConfirmationModal` (not antd's `Modal.confirm`).

- **Old selector:** `.sidebar-domain-section [href="/domain/${domainUrn}"] .anticon-close`
- **New selector:** `.sidebar-domain-section [data-testid="remove-icon"]`
- **Old confirm:** `getByText('Yes')` (antd modal default button text)
- **New confirm:** `[data-testid="modal-confirm-button"]` (ConfirmationModal uses this testid)

### Fix 2: `pages/domains.page.ts` — `deleteDomain`

**Root cause:** `DomainItemMenu` uses `ConfirmationModal` for domain deletion, which renders with `[data-testid="modal-confirm-button"]` rather than relying only on button text.

- Updated `getByText('Yes')` to use `[data-testid="modal-confirm-button"]` for reliability.

## Selector Verification (No Changes Needed)

All other selectors were verified against the current React source:

- `[data-testid="${urn}-health-icon"]` — exists in `app/previewV2/HealthIcon.tsx`
- `[data-testid="assertions-details"]` — exists in `app/previewV2/HealthPopover.tsx`
- `[data-testid="edit-documentation-button"]` — exists in `DocumentationTab.tsx`
- `[data-testid="description-editor-save-button"]` — exists in `DescriptionEditorToolbar.tsx`
- `[data-testid="add-related-button"]` — exists in `RelatedSection.tsx`
- `[data-testid="related-list"]` — exists in `RelatedSection.tsx`
- `[data-testid="url-input"]`, `[data-testid="label-input"]` — exist in `UrlLinkForm.tsx`, `LinkFormWrapper.tsx`
- `[data-testid="show-in-asset-preview-checkbox"]` — exists in `AddEditLinkModal.tsx` (via Checkbox component)
- `[data-testid="link-form-modal-submit-button"]` — exists in `AddEditLinkModal.tsx`
- `[data-testid="edit-link-button"]`, `[data-testid="remove-link-button"]` — exist in `RelatedLinkItem.tsx`
- `[data-testid="domains-new-domain-button"]` — exists in `ManageDomainsPageV2.tsx`
- `[data-testid="create-domain-name"]`, `[data-testid="create-domain-id"]`, `[data-testid="create-domain-button"]` — exist in `CreateDomainModal.tsx`
- `[data-testid="domain-batch-add"]` — exists in `EntityActions.tsx`
- `[data-testid="create-ingestion-source-button"]` — exists in both `ingest/` and `ingestV2/`
- `[data-testid="recipe-builder-yaml-button"]`, `[data-testid="recipe-builder-next-button"]` — exist in both
- `[data-testid="ingestion-schedule-next-button"]` — exists in both `CreateScheduleStep.tsx`
- `[data-testid="source-name-input"]`, `[data-testid="ingestion-source-save-button"]` — exist in both
- `[data-testid="ingestion-source-table-status"]` — exists in both
- `[data-testid="create-secret-button"]` — exists in both
- `[data-testid="secret-modal-*"]` — exist in both
- `data-test-id="delete-secret-action"`, `data-icon="delete"` — exist in `SecretsList.tsx`

## Success messages verified

- "Link Added", "Link Removed", "Link Updated" — in `useLinkUtils.ts`
- "Description Updated" — in `DescriptionEditor.tsx`
- "Updated!" — in `SchemaDescriptionField.tsx`
- "Removed Domain." — in `SidebarDomainSection.tsx` (via toast)
- "Added assets to Domain!" — in `EntityActions.tsx`
- "Successfully created ingestion source!" — in ingestion list
- "Successfully updated ingestion source!" — in ingestion list
- "Removed ingestion source." — in ingestion list
- "Successfully created Secret!" — in `SecretsList.tsx`
- "Removed secret." — in `SecretsList.tsx`
- "Created secret!" — in `CreateSecretButton.tsx`

## Files Modified

- `e2e-test/ui/playwright/pages/domains.page.ts`
