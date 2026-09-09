/**
 * Searching and paginating the columns drawn inside a lineage node.
 *
 * DATA SEEDING: static datasets from fixtures/data.json (auto-seeded via test.use below).
 *
 *   wide_upstream ──col_01──▶ wide, which has 25 columns: col_01 … col_25
 *
 * A node draws NUM_COLUMNS_PER_PAGE (10) columns at a time in schema order, so the wide table
 * takes three pages. The search box filters the whole column list, not just the page on screen,
 * and the pagination follows what is left.
 */

import { test, expect } from '../../fixtures/base-test';
import { LineageV3Page } from '../../pages/lineage-v3.page';
import { TIMEOUTS } from '../../utils/constants';
import { BASE_FEATURE_FLAGS, WIDE_URN } from './constants';

test.use({ featureName: 'lineage-controls' });

const PAGE_1_COLUMNS = [
  'col_01',
  'col_02',
  'col_03',
  'col_04',
  'col_05',
  'col_06',
  'col_07',
  'col_08',
  'col_09',
  'col_10',
];
const PAGE_3_COLUMNS = ['col_21', 'col_22', 'col_23', 'col_24', 'col_25'];

test.describe('column search and pagination within a lineage node', () => {
  let lineagePage: LineageV3Page;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    lineagePage = new LineageV3Page(page, logger, logDir);
    await apiMock.setFeatureFlags(BASE_FEATURE_FLAGS);

    await lineagePage.navigateToDatasetLineage(WIDE_URN);
    await lineagePage.waitForGraphToRender();
    // The minimap floats over the bottom-right of the canvas; an expanded node's pagination can
    // land under it, which intercepts the click. Let clicks pass through it.
    await lineagePage.page.addStyleTag({ content: '.react-flow__minimap { pointer-events: none !important; }' });
    await lineagePage.expandContractColumns(WIDE_URN);
    await expect(lineagePage.getColumnSearchInput(WIDE_URN)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await lineagePage.waitForViewportToSettle();
  });

  test('pages through the columns ten at a time', async () => {
    expect(await lineagePage.getShownColumnNames(WIDE_URN)).toEqual(PAGE_1_COLUMNS);
    // 25 columns over 10 per page: three page buttons
    // eslint-disable-next-line playwright/no-raw-locators -- antd pagination items have no test id of their own
    await expect(lineagePage.getColumnPagination(WIDE_URN).locator('li.ant-pagination-item')).toHaveCount(3);

    await lineagePage.goToColumnPage(WIDE_URN, 3);
    await expect
      .poll(() => lineagePage.getShownColumnNames(WIDE_URN), { timeout: TIMEOUTS.MEDIUM })
      .toEqual(PAGE_3_COLUMNS);

    await lineagePage.goToColumnPage(WIDE_URN, 1);
    await expect
      .poll(() => lineagePage.getShownColumnNames(WIDE_URN), { timeout: TIMEOUTS.MEDIUM })
      .toEqual(PAGE_1_COLUMNS);
  });

  test('searches across every column, not just the page on screen', async () => {
    // col_23 is on the last page, so a search that finds it proves the filter runs over the
    // whole column list rather than the ten columns currently drawn.
    await lineagePage.searchColumns(WIDE_URN, 'col_23');
    await expect
      .poll(() => lineagePage.getShownColumnNames(WIDE_URN), { timeout: TIMEOUTS.MEDIUM })
      .toEqual(['col_23']);
    // One match fits on a single page, so the pagination goes away
    await expect(lineagePage.getColumnPagination(WIDE_URN)).toHaveCount(0);

    // A prefix matches a set, still drawn in schema order
    await lineagePage.searchColumns(WIDE_URN, 'col_2');
    await expect
      .poll(() => lineagePage.getShownColumnNames(WIDE_URN), { timeout: TIMEOUTS.MEDIUM })
      .toEqual(['col_20', 'col_21', 'col_22', 'col_23', 'col_24', 'col_25']);

    // Clearing the search restores the full, paginated list
    await lineagePage.clearColumnSearch(WIDE_URN);
    await expect
      .poll(() => lineagePage.getShownColumnNames(WIDE_URN), { timeout: TIMEOUTS.MEDIUM })
      .toEqual(PAGE_1_COLUMNS);
    await expect(lineagePage.getColumnPagination(WIDE_URN)).toBeVisible();
  });
});
