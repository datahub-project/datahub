import { Page } from '@playwright/test';

export class GraphQLHelper {
  constructor(private page: Page) {}

  async executeQuery(query: string, variables?: any): Promise<any> {
    const response = await this.page.request.post('/api/v2/graphql', {
      data: {
        query,
        variables: variables || {},
      },
      headers: {
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok()) {
      throw new Error(`GraphQL request failed: ${response.status()} ${response.statusText()}`);
    }

    const text = await response.text();
    if (!text || text.trim() === '') {
      throw new Error('GraphQL response is empty');
    }

    return JSON.parse(text);
  }

  async waitForGraphQLResponse(operationName: string): Promise<any> {
    const response = await this.page.waitForResponse(
      (response) =>
        response.url().includes('/graphql') &&
        response.request().postDataJSON()?.operationName === operationName
    );
    return await response.json();
  }

  async mockGraphQLResponse(operationName: string, mockData: any): Promise<void> {
    await this.page.route('**/graphql', async (route) => {
      const request = route.request();
      const postData = request.postDataJSON();

      if (postData?.operationName === operationName) {
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({ data: mockData }),
        });
      } else {
        await route.continue();
      }
    });
  }

  async setThemeV2Enabled(isEnabled: boolean): Promise<void> {
    await this.page.route('**/api/v2/graphql', async (route) => {
      const request = route.request();
      const postData = request.postDataJSON();
      const operationName = postData?.operationName;

      const response = await route.fetch();
      const json = await response.json();

      if (operationName === 'appConfig') {
        json.data.appConfig.featureFlags.themeV2Enabled = isEnabled;
        json.data.appConfig.featureFlags.themeV2Default = isEnabled;
        json.data.appConfig.featureFlags.showNavBarRedesign = isEnabled;
      } else if (operationName === 'getMe') {
        json.data.me.corpUser.settings.appearance.showThemeV2 = isEnabled;
      }

      await route.fulfill({ response, json });
    });
  }
}
