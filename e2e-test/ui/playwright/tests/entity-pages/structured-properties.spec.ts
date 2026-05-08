/**
 * Structured properties tests — migrated from Cypress e2e/structured_properties/structured_properties.js
 *
 * Covers: create, update, delete, hide/show, add/remove/edit on entity,
 * add/remove/edit on schema field, and asset summary tab visibility.
 *
 * Tests are independent: each creates its own property and cleans up after itself.
 *
 * Prerequisites: SamplePlaywrightHdfsDataset must exist with field `shipment_info`.
 */

import { test, expect } from '../../fixtures/base-test';
import type { Page } from '@playwright/test';
import { withTimestamp } from '../../utils/random';

test.use({ featureName: 'entity-pages' });

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hdfs,SamplePlaywrightHdfsDataset,PROD)';
const DATASET_NAME = 'SamplePlaywrightHdfsDataset';
const FIELD_NAME = 'shipment_info';
const PROP_VALUE = 'Playwright structured prop value';
const UPDATED_PROP_VALUE = 'Updated Playwright structured prop value';

// ── Helpers ───────────────────────────────────────────────────────────────────

async function goToStructuredProperties(page: Page): Promise<void> {
  await page.goto('/structured-properties');
  await expect(page.getByText('Structured Properties')).toBeVisible({ timeout: 15000 });
}

async function createStructuredProperty(
  page: Page,
  name: string,
  entity: string,
): Promise<void> {
  await page.locator('[data-testid="structured-props-create-button"]').click();
  await page.locator('[data-testid="structured-props-input-name"] input').fill(name);
  await page.locator('[data-testid="structured-props-select-input-type"]').click();
  await page.locator('[data-testid="structured-props-property-type-options-list"]').getByText('Text').click();
  await page.locator('[data-testid="structured-props-select-input-applies-to"]').click();
  await page.locator('[data-testid="applies-to-options-list"]').getByText(entity).click();
  await page.locator('[data-testid="structured-props-create-update-button"]').click();
  await expect(page.getByText('created')).toBeVisible({ timeout: 15000 });
  await expect(page.getByText(name)).toBeVisible({ timeout: 10000 });
}

async function deleteStructuredProperty(
  page: Page,
  name: string,
): Promise<void> {
  await page.locator('[data-testid="search-bar-input"]').scrollIntoViewIfNeeded();
  await page.locator('[data-testid="search-bar-input"]').fill(name);
  const row = page.locator('[data-testid="structured-props-table"]').locator('tr').filter({ hasText: name });
  await row.locator('[data-testid="structured-props-more-options-icon"]').click();
  await page.locator('body .ant-dropdown-menu').getByText('Delete').click();
  await page.locator('[data-testid="modal-confirm-button"]').click();
  await expect(page.getByText('deleted')).toBeVisible({ timeout: 15000 });
}

async function addStructuredPropertyToEntity(
  page: Page,
  propName: string,
): Promise<void> {
  await page.goto(`/dataset/${encodeURIComponent(DATASET_URN)}`);
  await expect(page.getByText(DATASET_NAME).first()).toBeVisible({ timeout: 20000 });
  await page.locator('[data-testid="Properties-entity-tab-header"]').click();
  await page.locator('[data-testid="add-structured-prop-button"]').click();
  await page.locator('body .ant-dropdown-menu').waitFor({ state: 'visible' });
  await page.locator('body .ant-dropdown-menu').getByText(propName).click();
  await page.locator('[data-testid="structured-property-string-value-input"]').fill(PROP_VALUE);
  await page.locator('[data-testid="add-update-structured-prop-on-entity-button"]').click();
  await expect(page.getByText('added')).toBeVisible({ timeout: 15000 });
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test.describe('Verify manage structured properties functionalities', () => {
  test.beforeEach(async ({ page }) => {
    await goToStructuredProperties(page);
  });

  test('Verify creating a new structured property', async ({ page }) => {
    const name = withTimestamp('Cypress Structured Prop');

    await createStructuredProperty(page, name, 'Dataset');
    await expect(page.getByText(name)).toBeVisible();

    await deleteStructuredProperty(page, name);
  });

  test('Verify updating an existing structured property', async ({ page }) => {
    const name = withTimestamp('Cypress Structured Prop');
    const updatedName = withTimestamp('Updated Cypress Structured Prop');

    await createStructuredProperty(page, name, 'Dataset');

    await page.locator('[data-testid="structured-props-table"]').getByText(name).click();
    await expect(page.getByText(name)).toBeVisible();
    await expect(page.getByText('Dataset')).toBeVisible();

    await page.locator('[data-testid="structured-props-input-name"]').clear();
    await page.locator('[data-testid="structured-props-input-name"]').fill(updatedName);
    await page.locator('[data-testid="structured-props-input-description"]').click();
    await page.locator('[data-testid="structured-props-input-description"]').fill('Description of cypress property');
    await page.locator('[data-testid="structured-props-select-input-applies-to"]').click();
    await page.locator('[data-testid="applies-to-options-list"]').getByText('Dashboard').click();
    await page.locator('[data-testid="structured-props-create-update-button"]').click();
    await expect(page.getByText('updated')).toBeVisible({ timeout: 15000 });
    await expect(page.getByText(updatedName)).toBeVisible();
    await expect(page.getByText('Description of cypress property')).toBeVisible();
    await expect(page.getByText('Dashboard')).toBeVisible();

    await deleteStructuredProperty(page, updatedName);
  });

  test('Verify deleting a structured property', async ({ page }) => {
    const name = withTimestamp('Cypress Structured Prop');

    await createStructuredProperty(page, name, 'Dataset');
    await deleteStructuredProperty(page, name);
    await expect(page.getByText('deleted')).toBeVisible({ timeout: 15000 });
    await expect(page.getByText(name)).not.toBeAttached({ timeout: 10000 });
  });

  test('Verify the absence of hidden structured property', async ({ page }) => {
    const name = withTimestamp('Cypress Structured Prop');

    await createStructuredProperty(page, name, 'Dataset');
    await page.locator('[data-testid="structured-props-table"]').getByText(name).click();
    await page.locator('[data-testid="structured-props-hide-switch"]').click();
    await page.locator('[data-testid="structured-props-create-update-button"]').click();

    // Verify hidden property does not appear in entity add dropdown
    await page.goto(`/dataset/${encodeURIComponent(DATASET_URN)}`);
    await page.locator('[data-testid="Properties-entity-tab-header"]').click();
    await page.locator('[data-testid="add-structured-prop-button"]').click();
    await page.locator('body .ant-dropdown-menu').waitFor({ state: 'visible' });
    await expect(page.locator('body .ant-dropdown-menu').getByText(name)).not.toBeAttached({ timeout: 5000 });

    await goToStructuredProperties(page);
    await deleteStructuredProperty(page, name);
  });

  test('Verify adding a structured property to an entity', async ({ page }) => {
    const name = withTimestamp('Cypress Structured Prop');

    await createStructuredProperty(page, name, 'Dataset');
    await addStructuredPropertyToEntity(page, name);

    await expect(page.getByText(name)).toBeVisible({ timeout: 10000 });
    await expect(page.getByText(PROP_VALUE)).toBeVisible();

    await goToStructuredProperties(page);
    await deleteStructuredProperty(page, name);
  });

  test('Verify removing a structured property from an entity', async ({ page }) => {
    const name = withTimestamp('Cypress Structured Prop');

    await createStructuredProperty(page, name, 'Dataset');
    await addStructuredPropertyToEntity(page, name);

    const row = page.locator('td').filter({ hasText: PROP_VALUE }).first();
    await row.locator('xpath=following-sibling::td').locator('[data-testid="structured-prop-entity-more-icon"]').click();
    await page.locator('body .ant-dropdown-menu').getByText('Remove').click();
    await page.locator('[data-testid="modal-confirm-button"]').click();
    await expect(page.getByText('removed')).toBeVisible({ timeout: 15000 });
    await expect(page.getByText(PROP_VALUE)).not.toBeAttached({ timeout: 10000 });

    await goToStructuredProperties(page);
    await deleteStructuredProperty(page, name);
  });

  test('Verify editing a structured property on an entity', async ({ page }) => {
    const name = withTimestamp('Cypress Structured Prop');

    await createStructuredProperty(page, name, 'Dataset');
    await addStructuredPropertyToEntity(page, name);

    const row = page.locator('td').filter({ hasText: PROP_VALUE }).first();
    await row.locator('xpath=following-sibling::td').locator('[data-testid="structured-prop-entity-more-icon"]').click();
    await page.locator('body .ant-dropdown-menu').getByText('Edit').click();
    await page.locator('[data-testid="structured-property-string-value-input"]').clear();
    await page.locator('[data-testid="structured-property-string-value-input"]').fill(UPDATED_PROP_VALUE);
    await page.locator('[data-testid="add-update-structured-prop-on-entity-button"]').click();
    await expect(page.getByText('updated')).toBeVisible({ timeout: 15000 });
    await expect(page.getByText(UPDATED_PROP_VALUE)).toBeVisible();

    await goToStructuredProperties(page);
    await deleteStructuredProperty(page, name);
  });

  test('Verify adding a structured property to a schema field', async ({ page }) => {
    const name = withTimestamp('Field Structured property');

    await createStructuredProperty(page, name, 'Column');

    // Enable show in columns table
    await page.locator('[data-testid="structured-props-table"]').getByText(name).click();
    await page.locator('[data-testid="structured-props-show-in-columns-table-switch"]').click();
    await page.locator('[data-testid="structured-props-create-update-button"]').click();
    await expect(page.getByText('updated')).toBeVisible({ timeout: 15000 });

    // Add to field
    await page.goto(`/dataset/${encodeURIComponent(DATASET_URN)}`);
    await expect(page.getByText(DATASET_NAME).first()).toBeVisible({ timeout: 20000 });
    await expect(page.getByText(FIELD_NAME)).toBeVisible();
    await page.getByText(FIELD_NAME).click();
    await page.locator(`[data-testid="${name}-add-or-edit-button"]`).click();
    await page.locator('[data-testid="structured-property-string-value-input"]').fill(PROP_VALUE);
    await page.locator('[data-testid="add-update-structured-prop-on-entity-button"]').click();
    await expect(page.getByText('added')).toBeVisible({ timeout: 15000 });
    await expect(page.locator('.ant-drawer-content').getByText(PROP_VALUE)).toBeVisible();

    await goToStructuredProperties(page);
    await deleteStructuredProperty(page, name);
  });

  test('Verify updating a structured property on a schema field', async ({ page }) => {
    const name = withTimestamp('Field Structured property');

    await createStructuredProperty(page, name, 'Column');

    // Enable show in columns table
    await page.locator('[data-testid="structured-props-table"]').getByText(name).click();
    await page.locator('[data-testid="structured-props-show-in-columns-table-switch"]').click();
    await page.locator('[data-testid="structured-props-create-update-button"]').click();
    await expect(page.getByText('updated')).toBeVisible({ timeout: 15000 });

    // Add to field, then edit
    await page.goto(`/dataset/${encodeURIComponent(DATASET_URN)}`);
    await expect(page.getByText(DATASET_NAME).first()).toBeVisible({ timeout: 20000 });
    await page.getByText(FIELD_NAME).click();
    await page.locator(`[data-testid="${name}-add-or-edit-button"]`).click();
    await page.locator('[data-testid="structured-property-string-value-input"]').fill(PROP_VALUE);
    await page.locator('[data-testid="add-update-structured-prop-on-entity-button"]').click();
    await expect(page.getByText('added')).toBeVisible({ timeout: 15000 });

    // Edit
    await page.locator(`[data-testid="${name}-add-or-edit-button"]`).click();
    await page.locator('[data-testid="structured-property-string-value-input"]').clear();
    await page.locator('[data-testid="structured-property-string-value-input"]').fill(UPDATED_PROP_VALUE);
    await page.locator('[data-testid="add-update-structured-prop-on-entity-button"]').click();
    await expect(page.getByText('updated')).toBeVisible({ timeout: 15000 });
    await expect(page.locator('.ant-drawer-content').getByText(UPDATED_PROP_VALUE)).toBeVisible();

    await goToStructuredProperties(page);
    await deleteStructuredProperty(page, name);
  });

  test('Verify removing a structured property from a schema field', async ({ page }) => {
    const name = withTimestamp('Field Structured property');

    await createStructuredProperty(page, name, 'Column');

    // Enable show in columns table
    await page.locator('[data-testid="structured-props-table"]').getByText(name).click();
    await page.locator('[data-testid="structured-props-show-in-columns-table-switch"]').click();
    await page.locator('[data-testid="structured-props-create-update-button"]').click();
    await expect(page.getByText('updated')).toBeVisible({ timeout: 15000 });

    // Add to field, then remove
    await page.goto(`/dataset/${encodeURIComponent(DATASET_URN)}`);
    await expect(page.getByText(DATASET_NAME).first()).toBeVisible({ timeout: 20000 });
    await page.getByText(FIELD_NAME).click();
    await page.locator(`[data-testid="${name}-add-or-edit-button"]`).click();
    await page.locator('[data-testid="structured-property-string-value-input"]').fill(PROP_VALUE);
    await page.locator('[data-testid="add-update-structured-prop-on-entity-button"]').click();
    await expect(page.getByText('added')).toBeVisible({ timeout: 15000 });

    // Switch to Properties drawer tab
    await page.locator('[data-testid="Properties-field-drawer-tab-header"]').click();

    const fieldRow = page.locator('td').filter({ hasText: name }).first();
    await fieldRow.locator('xpath=following-sibling::td').locator('[data-testid="structured-prop-entity-more-icon"]').click();
    await page.locator('body .ant-dropdown-menu').getByText('Remove').click();
    await page.locator('[data-testid="modal-confirm-button"]').click();
    await expect(page.getByText('removed')).toBeVisible({ timeout: 15000 });
    await expect(page.locator('.ant-drawer-content').getByText(PROP_VALUE)).not.toBeAttached({ timeout: 10000 });

    await goToStructuredProperties(page);
    await deleteStructuredProperty(page, name);
  });

  test('Verify property is available in asset summary tab when empty', async ({ page }) => {
    const name = withTimestamp('Property-showInSummaryTab');

    await createStructuredProperty(page, name, 'Dataset');

    // Enable show in asset summary
    await page.locator('[data-testid="structured-props-table"]').getByText(name).click();
    await page.locator('[data-testid="structured-props-show-in-asset-summary-switch"]').click();
    await page.locator('[data-testid="structured-props-create-update-button"]').click();
    await expect(page.getByText('updated')).toBeVisible({ timeout: 15000 });

    // Navigate to dataset and open Summary tab in sidebar
    await page.goto(`/dataset/${encodeURIComponent(DATASET_URN)}`);
    await expect(page.getByText(DATASET_NAME).first()).toBeVisible({ timeout: 20000 });

    const summaryTab = page.locator('#entity-sidebar-tabs-tab-Summary');
    const isSelected = await summaryTab.getAttribute('aria-selected');
    if (isSelected !== 'true') {
      await summaryTab.click();
    }

    await expect(page.locator('#entity-profile-v2-sidebar').getByText(name)).toBeVisible({ timeout: 10000 });

    await goToStructuredProperties(page);
    await deleteStructuredProperty(page, name);
  });

  test('Verify property is not available in asset summary tab when it\'s empty (hide when empty enabled)', async ({
    page,
  }) => {
    const name = withTimestamp('Property-hideInSummaryTabWithoutValue');

    await createStructuredProperty(page, name, 'Dataset');

    // Enable show in asset summary
    await page.locator('[data-testid="structured-props-table"]').getByText(name).click();
    await page.locator('[data-testid="structured-props-show-in-asset-summary-switch"]').click();
    await page.locator('[data-testid="structured-props-create-update-button"]').click();
    await expect(page.getByText('updated')).toBeVisible({ timeout: 15000 });

    // Enable hide when empty
    await page.locator('[data-testid="structured-props-hide-in-asset-summary-when-empty-checkbox"]').click({ force: true });
    await page.locator('[data-testid="structured-props-create-update-button"]').click();
    await expect(page.getByText('updated')).toBeVisible({ timeout: 15000 });

    // Navigate to dataset
    await page.goto(`/dataset/${encodeURIComponent(DATASET_URN)}`);
    await expect(page.getByText(DATASET_NAME).first()).toBeVisible({ timeout: 20000 });

    const summaryTab = page.locator('#entity-sidebar-tabs-tab-Summary');
    const isSelected = await summaryTab.getAttribute('aria-selected');
    if (isSelected !== 'true') {
      await summaryTab.click();
    }

    await expect(page.locator('#entity-profile-v2-sidebar').getByText(name)).not.toBeAttached({ timeout: 10000 });

    await goToStructuredProperties(page);
    await deleteStructuredProperty(page, name);
  });

  test('Verify property is available in asset summary tab when it isn\'t empty', async ({ page }) => {
    const name = withTimestamp('Property-hideInSummaryTabWhenEmptyWithValue');

    await createStructuredProperty(page, name, 'Dataset');

    // Enable show in asset summary
    await page.locator('[data-testid="structured-props-table"]').getByText(name).click();
    await page.locator('[data-testid="structured-props-show-in-asset-summary-switch"]').click();
    await page.locator('[data-testid="structured-props-create-update-button"]').click();
    await expect(page.getByText('updated')).toBeVisible({ timeout: 15000 });

    // Enable hide when empty
    await page.locator('[data-testid="structured-props-hide-in-asset-summary-when-empty-checkbox"]').click({ force: true });
    await page.locator('[data-testid="structured-props-create-update-button"]').click();
    await expect(page.getByText('updated')).toBeVisible({ timeout: 15000 });

    // Add a value to the entity
    await addStructuredPropertyToEntity(page, name);

    // Open Summary tab in sidebar
    const summaryTab = page.locator('#entity-sidebar-tabs-tab-Summary');
    const isSelected = await summaryTab.getAttribute('aria-selected');
    if (isSelected !== 'true') {
      await summaryTab.click();
    }

    await expect(page.locator('#entity-profile-v2-sidebar').getByText(name)).toBeVisible({ timeout: 10000 });

    await goToStructuredProperties(page);
    await deleteStructuredProperty(page, name);
  });
});
