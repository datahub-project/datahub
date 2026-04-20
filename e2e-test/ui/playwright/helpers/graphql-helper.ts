import { Page } from '@playwright/test';

export class GraphQLHelper {
  constructor(private page: Page) {}

  async executeQuery(
    query: string,
    variables?: Record<string, unknown>,
  ): Promise<Record<string, unknown>> {
    const response = await this.page.request.post('/api/v2/graphql', {
      data: {
        query,
        variables: variables ?? {},
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

    return JSON.parse(text) as Record<string, unknown>;
  }

  async waitForGraphQLResponse(operationName: string): Promise<Record<string, unknown>> {
    const response = await this.page.waitForResponse(
      (r) =>
        r.url().includes('/graphql') &&
        (r.request().postDataJSON() as Record<string, unknown> | null)?.operationName ===
          operationName,
    );
    return response.json() as Promise<Record<string, unknown>>;
  }
}
