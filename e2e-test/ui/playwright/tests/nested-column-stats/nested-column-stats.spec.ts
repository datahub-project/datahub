/**
 * Regression test for ING-2174: BigQuery nested field sample data missing in Columns tab.
 *
 * Root cause: SchemaFieldDrawer.tsx used an exact string match to resolve fieldProfile
 * from latestProfile.fieldProfiles. BigQuery schema fields use V2 paths
 * (e.g. [version=2.0].[type=struct].address) while the profiler emits V1 paths
 * (e.g. address). For nested fields the exact match always failed, so fieldProfile
 * was undefined and the Statistics tab showed "No column statistics found".
 *
 * Fix: use pathMatchesInsensitiveToV2 which normalises both sides before comparing,
 * also folding case so that lowercased schema paths (produced by BigQuery ingestion
 * with convert_column_urns_to_lowercase=true) match camelCase profiler paths.
 *
 * Fixture: tests/nested-column-stats/fixtures/data.json
 *   - SchemaMetadata with V2 field paths (as BigQuery ingestion emits)
 *   - DatasetProfile with V1 field paths (as ge_data_profiler emits)
 *     The "address" struct field has profile path "address" (V1) but schema path
 *     "[version=2.0].[type=struct].address" (V2).
 *   - Additionally, "rawcounterpartyid" (top-level) demonstrates the case-fold fix:
 *     schema path is fully lowercased while the profiler path retains the original
 *     camelCase ("rawCounterpartyId").
 */

import { test, expect } from '../../fixtures/base-test';

test.use({ featureName: 'nested-column-stats' });

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:bigquery,playwright_test.nested_schema.orders,PROD)';

const TOP_LEVEL_FIELD = 'order_id';

test.describe('ING-2174: nested BigQuery field stats in Columns tab', () => {
  test('nested struct field shows sample values in Statistics tab (not "No column statistics found")', async ({
    page,
  }) => {
    await page.goto(`/dataset/${encodeURIComponent(DATASET_URN)}/Schema`);
    await page.waitForLoadState('networkidle');

    // Wait for schema table to render
    // eslint-disable-next-line playwright/no-raw-locators -- id-prefix attribute selector has no semantic equivalent
    await expect(page.locator('[id^="column-"]').first()).toBeVisible({ timeout: 30000 });

    // Click the 'address' struct row. Its data-testid contains a V2 path with brackets,
    // so use XPath for reliable exact attribute matching.
    // eslint-disable-next-line playwright/no-raw-locators -- XPath required to match data-testid containing reserved characters ([], =, .)
    const addressRow = page.locator('xpath=//tr[@data-testid="column-[version=2.0].[type=struct].address"]');
    await expect(addressRow).toBeVisible({ timeout: 10000 });
    await addressRow.click();

    // Open the Statistics tab in the schema field drawer
    await page.locator('[data-testid="Statistics-field-drawer-tab-header"]').click();

    // Pre-fix: "No column statistics found" appears — V2 fieldPath can't match V1 profile path
    // Post-fix: sample values from the profile are shown
    await expect(page.getByText('No column statistics found')).not.toBeVisible({ timeout: 10000 });
    await expect(page.getByText('123 Main St Seattle')).toBeVisible({ timeout: 10000 });
  });

  test('lowercased schema path (ING-2174 case-fold) shows sample values in About tab from Columns tab', async ({
    page,
  }) => {
    // The schema path is lowercased by convert_column_urns_to_lowercase=true while the profiler
    // retains the original camelCase. The field is top-level so no struct expansion is needed.
    await page.goto(`/dataset/${encodeURIComponent(DATASET_URN)}/Schema`);
    await page.waitForLoadState('networkidle');

    // eslint-disable-next-line playwright/no-raw-locators -- id-prefix attribute selector has no semantic equivalent
    await expect(page.locator('[id^="column-"]').first()).toBeVisible({ timeout: 30000 });

    // Click the lowercased top-level field — opens the drawer on the About tab (default).
    // eslint-disable-next-line playwright/no-raw-locators -- XPath required to match data-testid containing reserved characters ([], =, .)
    const rawCounterpartyRow = page.locator(
      'xpath=//tr[@data-testid="column-[version=2.0].[type=string].rawcounterpartyid"]',
    );
    await expect(rawCounterpartyRow).toBeVisible({ timeout: 10000 });
    await rawCounterpartyRow.click();

    // About tab is the default — no tab click needed.
    // Pre-fix: pathMatchesInsensitiveToV2 was case-sensitive, so fieldProfile was undefined
    //          and the About section showed "No column statistics found".
    // Post-fix: case-folded match resolves fieldProfile correctly and sample values render.
    await expect(page.getByText('No column statistics found')).not.toBeVisible({ timeout: 10000 });
    await expect(page.getByText('RAW-001')).toBeVisible({ timeout: 10000 });
  });

  test('top-level field still shows stats correctly (regression guard)', async ({ page }) => {
    await page.goto(`/dataset/${encodeURIComponent(DATASET_URN)}/Schema`);
    await page.waitForLoadState('networkidle');

    // eslint-disable-next-line playwright/no-raw-locators -- id-prefix attribute selector has no semantic equivalent
    await expect(page.locator('[id^="column-"]').first()).toBeVisible({ timeout: 30000 });

    await page.locator(`[data-testid="column-${TOP_LEVEL_FIELD}"]`).click();
    await page.locator('[data-testid="Statistics-field-drawer-tab-header"]').click();

    await expect(page.getByText('No column statistics found')).not.toBeVisible({ timeout: 10000 });
    await expect(page.getByText('Sample Values')).toBeVisible({ timeout: 10000 });
  });
});
