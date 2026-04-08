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

}
