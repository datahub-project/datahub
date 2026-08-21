import { Page } from '@playwright/test';
import { test, expect } from '../../fixtures/base-test';
import { AssertionListPage } from '../../pages/assertion-list.page';

test.use({ featureName: 'quality' });

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,playwright_quality_assertion_list,PROD)';
const PLATFORM_URN = 'urn:li:dataPlatform:hive';

type AssertionSearchResponse = {
  searchAcrossEntities: {
    searchResults: Array<{
      entity: {
        urn: string;
      };
    }>;
  };
};

async function executeGraphQL<T>(page: Page, query: string, variables: Record<string, unknown> = {}): Promise<T> {
  return page.evaluate(
    async ({ graphqlQuery, graphqlVariables }) => {
      const response = await fetch('/api/graphql', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: graphqlQuery, variables: graphqlVariables }),
      });
      const payload = await response.json();
      if (!response.ok || payload.errors) {
        throw new Error(JSON.stringify(payload.errors || payload));
      }
      return payload.data;
    },
    { graphqlQuery: query, graphqlVariables: variables },
  );
}

test('renders and searches the paginated Quality assertion list', async ({ page, logger, logDir }) => {
  test.setTimeout(120_000);
  await page.goto('/');

  const description = `Headless assertion ${Date.now()}`;
  const created = await executeGraphQL<{ upsertCustomAssertion: { urn: string } }>(
    page,
    `mutation CreateBrowserAssertion($input: UpsertCustomAssertionInput!) {
      upsertCustomAssertion(input: $input) {
        urn
      }
    }`,
    {
      input: {
        entityUrn: DATASET_URN,
        type: 'Headless Check',
        description,
        platform: {
          urn: PLATFORM_URN,
          name: 'hive',
        },
      },
    },
  );

  try {
    await expect
      .poll(
        async () => {
          try {
            const result = await executeGraphQL<{ reportAssertionResult: boolean }>(
              page,
              `mutation ReportBrowserAssertion($urn: String!) {
                reportAssertionResult(urn: $urn, result: { type: SUCCESS })
              }`,
              { urn: created.upsertCustomAssertion.urn },
            );
            return result.reportAssertionResult;
          } catch (error) {
            if (error instanceof Error && error.message.includes('does not exist or is not associated')) {
              return false;
            }
            throw error;
          }
        },
        {
          message: 'created assertion should accept a run result',
          timeout: 30_000,
          intervals: [500, 1000, 2000],
        },
      )
      .toBe(true);

    await expect
      .poll(
        async () => {
          const result = await executeGraphQL<AssertionSearchResponse>(
            page,
            `query SearchBrowserAssertion($input: SearchAcrossEntitiesInput!) {
              searchAcrossEntities(input: $input) {
                searchResults {
                  entity {
                    urn
                  }
                }
              }
            }`,
            {
              input: {
                types: ['ASSERTION'],
                query: '*',
                start: 0,
                count: 10,
                orFilters: [
                  {
                    and: [{ field: 'entity', values: [DATASET_URN], condition: 'EQUAL' }],
                  },
                ],
                searchFlags: { skipCache: true },
              },
            },
          );
          return result.searchAcrossEntities.searchResults.some(
            ({ entity }) => entity.urn === created.upsertCustomAssertion.urn,
          );
        },
        {
          message: 'created assertion should be available in search',
          timeout: 30_000,
          intervals: [500, 1000, 2000],
        },
      )
      .toBe(true);

    const assertionList = new AssertionListPage(page, logger, logDir);
    await assertionList.navigateToDatasetAssertions(DATASET_URN);
    await assertionList.expectAssertionVisible(description);

    await assertionList.search(description);
    await assertionList.expectAssertionVisible(description);
  } finally {
    await executeGraphQL(
      page,
      `mutation DeleteBrowserAssertion($urn: String!) {
        deleteAssertion(urn: $urn)
      }`,
      { urn: created.upsertCustomAssertion.urn },
    );
  }
});
