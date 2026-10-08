import type { Page } from '@playwright/test';

import { expect, test } from '../../fixtures/base-test';
import { GraphQLHelper } from '../../helpers/graphql-helper';
import { ViewSelectPage } from '../../pages/views/view-select.page';
import { TIMEOUTS } from '../../utils/constants';
import { withRandomSuffix } from '../../utils/random';

/**
 * Embedded lists ("View All Physical Children", a user's "Owner Of" tab) apply the view selected in
 * the search bar, with the All / view switcher, as other embedded lists do.
 */

test.use({ featureName: 'logical-models' });

const LOGICAL_PARENT_URN = 'urn:li:dataset:(urn:li:dataPlatform:logical,petshop.pet_orders,PROD)';
const OWNER_URN = 'urn:li:corpuser:petshop-owner';
const INCLUDED_CHILD_NAME = 'petshop_store_01.pet_orders';
const EXCLUDED_CHILD_NAME = 'petshop_warehouse.pet_orders';
const TOTAL_CHILDREN = 12;
const POSTGRES_CHILDREN = 11;

// Matches the pager's "1 - 10 of 12" text; the page-number buttons alone can also read "12".
function resultRange(total: number): RegExp {
  return new RegExp(`^1 - ${Math.min(total, 10)} of ${total}$`);
}

const LIST_MY_VIEWS = `
  query listMyViews($input: ListMyViewsInput!) {
    listMyViews(input: $input) {
      views {
        urn
      }
    }
  }
`;

const CREATE_VIEW = `
  mutation createView($input: CreateViewInput!) {
    createView(input: $input) {
      urn
    }
  }
`;

// Creates a postgres-only view and waits for the view picker to list it.
async function createStoreDatabasesView(page: Page, track: (urn: string) => void): Promise<string> {
  const viewName = withRandomSuffix('StoreDatabases');
  const graphql = new GraphQLHelper(page);
  const created = (await graphql.executeQuery(CREATE_VIEW, {
    input: {
      viewType: 'PERSONAL',
      name: viewName,
      definition: {
        entityTypes: [],
        filter: {
          operator: 'AND',
          filters: [{ field: 'platform', values: ['urn:li:dataPlatform:postgres'] }],
        },
      },
    },
  })) as { data?: { createView?: { urn?: string } } };
  const viewUrn = created.data?.createView?.urn;
  expect(viewUrn, 'createView should return a urn').toBeTruthy();
  track(viewUrn as string);

  // The view picker lists views from the search index, so wait until the new view is indexed.
  await expect
    .poll(
      async () => {
        const listed = (await graphql.executeQuery(LIST_MY_VIEWS, { input: { start: 0, count: 1000 } })) as {
          data?: { listMyViews?: { views?: Array<{ urn: string }> } };
        };
        return listed.data?.listMyViews?.views?.some((view) => view.urn === viewUrn);
      },
      { timeout: TIMEOUTS.LONG },
    )
    .toBe(true);

  return viewName;
}

test.describe('Embedded lists apply the search bar view', () => {
  test('View All Physical Children', async ({ page, cleanup }) => {
    const viewName = await createStoreDatabasesView(page, (urn) => cleanup.track(urn));
    await page.goto(`/dataset/${encodeURIComponent(LOGICAL_PARENT_URN)}`);
    await new ViewSelectPage(page).selectView(viewName);

    await page
      .getByTestId('physical-children-list')
      .getByText(`and ${TOTAL_CHILDREN - 10} more`)
      .click();
    const modal = page.getByRole('dialog').filter({ hasText: 'View All Physical Children' });
    const pagination = modal.getByTestId('embedded-list-search-pagination');
    const search = modal.getByPlaceholder('Search entities...');

    // The view is offered and applied: the Snowflake child is filtered out.
    await expect(modal.getByText(`Only showing entities in the ${viewName} view.`)).toBeVisible();
    await expect(pagination.getByText(resultRange(POSTGRES_CHILDREN))).toBeVisible({ timeout: TIMEOUTS.LONG });

    // Searching within the view finds a child in the view, and not the excluded one.
    await search.fill('store_01');
    await page.keyboard.press('Enter');
    await expect(modal.getByText(INCLUDED_CHILD_NAME, { exact: true })).toBeVisible({ timeout: TIMEOUTS.LONG });
    await search.fill('warehouse');
    await page.keyboard.press('Enter');
    await expect(modal.getByText('No results found')).toBeVisible({ timeout: TIMEOUTS.LONG });

    // "All" lists every child, the excluded one included.
    await modal.getByText('All', { exact: true }).click();
    await expect(modal.getByText(EXCLUDED_CHILD_NAME, { exact: true })).toBeVisible({ timeout: TIMEOUTS.LONG });
    await search.fill('');
    await page.keyboard.press('Enter');
    await expect(pagination.getByText(resultRange(TOTAL_CHILDREN))).toBeVisible({ timeout: TIMEOUTS.LONG });
  });

  test("a user's Owner Of tab", async ({ page, cleanup }) => {
    const viewName = await createStoreDatabasesView(page, (urn) => cleanup.track(urn));
    await page.goto(`/user/${encodeURIComponent(OWNER_URN)}/owner of`);
    await new ViewSelectPage(page).selectView(viewName);
    // The profile sidebar has its own Owner Of preview, which ignores views; check the tab's results.
    const results = page.getByTestId('embedded-list-search-results');

    // The user owns one postgres child and the Snowflake child; the view lists only the first.
    await expect(page.getByText(`Only showing entities in the ${viewName} view.`)).toBeVisible({
      timeout: TIMEOUTS.LONG,
    });
    await expect(results.getByText(INCLUDED_CHILD_NAME, { exact: true })).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(results.getByText(EXCLUDED_CHILD_NAME, { exact: true })).toHaveCount(0);

    // "All" lists both.
    await page.getByText('All', { exact: true }).click();
    await expect(results.getByText(EXCLUDED_CHILD_NAME, { exact: true })).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(results.getByText(INCLUDED_CHILD_NAME, { exact: true })).toBeVisible();
  });
});
