import { expect, test } from '../../fixtures/base-test';
import { GraphQLHelper } from '../../helpers/graphql-helper';
import { TIMEOUTS } from '../../utils/constants';
import { withRandomSuffix } from '../../utils/random';

/**
 * "View All Physical Children" lists every physical child of the logical dataset, whatever view is
 * selected in the search bar, and never offers that view, which this list does not apply.
 */

test.use({ featureName: 'logical-models' });

const LOGICAL_PARENT_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,petshop.pet_orders,PROD)';
const EXCLUDED_CHILD_NAME = 'petshop_warehouse.pet_orders';
const TOTAL_CHILDREN = 12;

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

test.describe('Physical children modal', () => {
  test('lists every child and does not offer the search bar view', async ({ page, cleanup }) => {
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
    cleanup.track(viewUrn as string);

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

    await page.goto(`/dataset/${encodeURIComponent(LOGICAL_PARENT_URN)}`);
    await page.getByTestId('views-button').click();
    // Other views may push this one out of the visible row, so narrow the list first.
    await page.getByTestId('views-popover').getByPlaceholder('Search views...').fill(viewName);
    await page.getByTestId('views-popover').getByTestId('view-select-item').filter({ hasText: viewName }).click();
    await expect(page.getByTestId('views-button')).toContainText(viewName);

    await page
      .getByTestId('physical-children-list')
      .getByText(`and ${TOTAL_CHILDREN - 10} more`)
      .click();

    const modal = page.getByRole('dialog').filter({ hasText: 'View All Physical Children' });
    const pagination = modal.getByTestId('embedded-list-search-pagination');

    // Every child is listed, and the search bar's view is not offered in this list.
    await expect(pagination.getByText(resultRange(TOTAL_CHILDREN))).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(modal.getByText(viewName)).toHaveCount(0);
    await expect(modal.getByTestId('embedded-list-active-filters')).toHaveCount(0);

    // A child the selected view would exclude is still a child, and still listed.
    await modal.getByPlaceholder('Search entities...').fill('warehouse');
    await page.keyboard.press('Enter');
    await expect(modal.getByText(EXCLUDED_CHILD_NAME, { exact: true })).toBeVisible({ timeout: TIMEOUTS.LONG });
  });
});
