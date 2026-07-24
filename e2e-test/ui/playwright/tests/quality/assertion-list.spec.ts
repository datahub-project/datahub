import { Page } from '@playwright/test';
import { test, expect } from '../../fixtures/login-test';
import { AssertionListPage } from '../../pages/assertion-list.page';

type DatasetSearchResponse = {
  searchAcrossEntities: {
    searchResults: Array<{
      entity: {
        urn: string;
        platform: {
          urn: string;
          name: string;
        };
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

test('renders and searches the paginated Quality assertion list', async ({ page, loginPage, logger, logDir }) => {
  test.setTimeout(120_000);
  await loginPage.navigateToLogin();
  await loginPage.usernameInput.fill('datahub');
  await loginPage.passwordInput.fill('datahub');
  await Promise.all([
    page.waitForURL((url) => !url.pathname.includes('login'), { timeout: 30_000 }),
    loginPage.loginButton.click({ force: true }),
  ]);

  const datasetData = await executeGraphQL<DatasetSearchResponse>(
    page,
    `query BrowserDataset {
      searchAcrossEntities(input: { types: [DATASET], query: "*", start: 0, count: 1 }) {
        searchResults {
          entity {
            ... on Dataset {
              urn
              platform {
                urn
                name
              }
            }
          }
        }
      }
    }`,
  );
  const dataset = datasetData.searchAcrossEntities.searchResults[0]?.entity;
  expect(dataset, 'quickstart should contain a dataset').toBeTruthy();

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
        entityUrn: dataset.urn,
        type: 'Headless Check',
        description,
        platform: {
          urn: dataset.platform.urn,
          name: dataset.platform.name,
        },
      },
    },
  );

  try {
    await executeGraphQL(
      page,
      `mutation ReportBrowserAssertion($urn: String!) {
        reportAssertionResult(urn: $urn, result: { type: SUCCESS })
      }`,
      { urn: created.upsertCustomAssertion.urn },
    );
    await page.waitForTimeout(5000);

    const assertionList = new AssertionListPage(page, logger, logDir);
    await assertionList.navigateToDatasetAssertions(dataset.urn);
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
