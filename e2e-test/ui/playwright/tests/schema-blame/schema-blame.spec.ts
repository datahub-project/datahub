import { test, expect } from '../../fixtures/base-test';
import { SchemaBlamePage } from '../../pages/schema-blame.page';

/**
 * Schema Blame Tests - Full Coverage Migration from Cypress
 *
 * Tests for the schema blame/timeline feature which allows viewing
 * schema changes across different dataset versions.
 *
 * Uses test data: SchemaBlameTesterDataset with two schema versions
 * Fixture data format mirrors Cypress (two MCEs for version history):
 *   - Version 0 (older): field_foo, field_bar
 *   - Version 0 (latest): field_foo (with Legacy tag), field_baz
 *
 * Test Coverage Equivalent to Cypress:
 * ✓ Latest version field presence/absence
 * ✓ Field descriptions
 * ✓ Field-level tags
 * ✓ Version switching
 * ✓ Schema blame/timeline toggle
 * ✓ History tab verification
 * ✓ Raw view functionality
 */

test.use({ featureName: 'schema-blame' });

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,SchemaBlameTesterDataset,PROD)';

test.describe('schema blame', () => {
  test('can activate the blame view and verify for the latest version of a dataset', async ({
    page,
    logger,
    logDir,
  }) => {
    const schemaPage = new SchemaBlamePage(page, logger, logDir);

    // Navigate to dataset
    await schemaPage.navigateToDataset(DATASET_URN);
    await schemaPage.closeModals();

    // ── Verify latest version (version 0 with field_baz) ──
    // Latest version should have: field_foo, field_baz (NOT field_bar)
    await schemaPage.verifyFieldVisible('field_foo');
    await schemaPage.verifyFieldVisible('field_baz');
    await schemaPage.verifyFieldNotVisible('field_bar');

    // ── Verify field descriptions ──
    await expect(page.getByText('Foo field description has changed')).toBeVisible();
    await expect(page.getByText('Baz field description')).toBeVisible();

    // ── Verify Legacy tag on field_foo ──
    await schemaPage.clickField('field_foo');
    // Use the verifyFieldHasTag method instead of generic getByText
    await schemaPage.verifyFieldHasTag('field_foo', 'Legacy');

    // ── Open version selector and select older version ──
    await schemaPage.openVersionSelector();
    await schemaPage.selectVersion('0');

    // ── Activate schema blame/timeline view ──
    await schemaPage.toggleSchemaBlame();

    // ── Verify fields in schema blame view ──
    // Should show both field_foo and field_bar in the older version
    await schemaPage.verifyFieldVisible('field_foo');
    await schemaPage.verifyFieldVisible('field_bar');

    // ── Verify descriptions in schema blame view ──
    // Check that descriptions are visible after switching to older version and toggling blame
    await schemaPage.verifyFieldDescriptionContains('Foo field description');
    await schemaPage.verifyFieldDescriptionContains('Bar field description');

    // ── Verify History tab exists ──
    await schemaPage.verifySchemaBlameOpen();
  });

  test('can activate the blame view and verify for an older version of a dataset', async ({ page, logger, logDir }) => {
    const schemaPage = new SchemaBlamePage(page, logger, logDir);

    // Navigate to dataset
    await schemaPage.navigateToDataset(DATASET_URN);

    // ── Verify field_baz is visible in latest version ──
    await schemaPage.verifyFieldVisible('field_baz');

    // Close any modals
    await schemaPage.closeModals();

    // ── Open version selector and select older version ──
    await schemaPage.openVersionSelector();
    await schemaPage.selectVersion('0');

    // ── Verify older version (version 0 with field_bar) ──
    // Older version should have: field_foo, field_bar (NOT field_baz)
    await expect(page.getByText('field_foo')).toBeVisible();
    await expect(page.getByText('field_bar')).toBeVisible();
    await schemaPage.verifyFieldNotVisible('field_baz');

    // ── Verify field descriptions in older version ──
    await expect(page.getByText('Foo field description')).toBeVisible();
    await expect(page.getByText('Bar field description')).toBeVisible();

    // ── Verify Legacy tag does NOT exist on field_foo in older version ──
    await schemaPage.clickField('field_foo');
    await schemaPage.verifyFieldDoesNotHaveTag('field_foo', 'Legacy');

    // ── Verify raw view button exists ──
    await schemaPage.verifyRawViewButtonExists();
  });
});
