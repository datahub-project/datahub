/**
 * Links to a column open that column.
 *
 * DataHub links to a column as `/dataset/<urn>/Columns?highlightedPath=<fieldPath>` (column search
 * results, lineage, "replaced by" deprecation notes, structured property values pointing at a column,
 * and a column's own `/schemaField/<urn>` page all lead there). Following such a link must land on the
 * Columns tab with that column's drawer open, and the drawer must still close when the user moves to
 * another tab.
 *
 * Seeds one dataset from tests/column-deep-link/fixtures/data.json.
 */

import { test, expect } from '../../fixtures/base-test';
import { LOAD_STATES, TIMEOUTS } from '../../utils/constants';
import { DomainTag } from '../../utils/test-tags';

test.use({ featureName: 'column-deep-link' });

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:postgres,petshop.public.pet_orders,PROD)';
const FIELD = 'pet_name';
const FIELD_DESCRIPTION = 'Name of the pet ordered';

const columnLink = (fieldPath: string) =>
  `/dataset/${encodeURIComponent(DATASET_URN)}/Columns?highlightedPath=${encodeURIComponent(fieldPath)}`;

test.describe('Links to a column', { tag: [DomainTag.Catalog] }, () => {
  test('open the Columns tab with that column selected', async ({ page }) => {
    await page.goto(columnLink(FIELD));
    await page.waitForLoadState(LOAD_STATES.DOMCONTENTLOADED);
    // The Columns tab has rendered the column, so a missing drawer below is about the selection, not loading.
    await expect(page.getByTestId(`schema-field-${FIELD}`)).toBeVisible({ timeout: TIMEOUTS.EXTRA_LONG });

    const drawer = page.getByTestId('schema-field-drawer-content');
    await expect(drawer.getByText(FIELD_DESCRIPTION)).toBeVisible({ timeout: TIMEOUTS.LONG });
    // The link stays shareable: the selected column remains in the URL.
    await expect(page).toHaveURL(new RegExp(`highlightedPath=${FIELD}(&|$)`));
  });

  test("open from a column's own page", async ({ page }) => {
    const schemaFieldUrn = `urn:li:schemaField:(${DATASET_URN},${FIELD})`;
    await page.goto(`/schemaField/${encodeURIComponent(schemaFieldUrn)}`);
    await page.waitForLoadState(LOAD_STATES.DOMCONTENTLOADED);
    await expect(page.getByTestId(`schema-field-${FIELD}`)).toBeVisible({ timeout: TIMEOUTS.EXTRA_LONG });

    const drawer = page.getByTestId('schema-field-drawer-content');
    await expect(drawer.getByText(FIELD_DESCRIPTION)).toBeVisible({ timeout: TIMEOUTS.LONG });
  });

  // The reason the Columns tab resets its selection on navigation: a column opened there must not stay
  // open behind another tab.
  test('close the column when the user moves to another tab', async ({ page }) => {
    await page.goto(`/dataset/${encodeURIComponent(DATASET_URN)}/Columns`);
    await page.getByTestId(`schema-field-${FIELD}`).click();
    const drawer = page.getByTestId('schema-field-drawer-content');
    await expect(drawer.getByText(FIELD_DESCRIPTION)).toBeVisible({ timeout: TIMEOUTS.EXTRA_LONG });

    await page.getByTestId('Lineage-entity-tab-header').click();
    await expect(drawer).toBeHidden();
  });
});
