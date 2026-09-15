import { Page } from '@playwright/test';

import { DATAHUB_GRAPHQL_PATH } from '../utils/constants';

export type GraphQLResponse = Record<string, unknown>;

export class GraphQLHelper {
  constructor(private page: Page) {}

  async executeQuery(query: string, variables?: Record<string, unknown>): Promise<GraphQLResponse> {
    const response = await this.page.request.post(DATAHUB_GRAPHQL_PATH, {
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

    return JSON.parse(text) as GraphQLResponse;
  }

  async waitForGraphQLResponse(operationName: string): Promise<GraphQLResponse> {
    const response = await this.page.waitForResponse(
      (r) =>
        r.url().includes('/graphql') &&
        (r.request().postDataJSON() as Record<string, unknown> | null)?.operationName === operationName,
    );
    return response.json() as Promise<GraphQLResponse>;
  }
}
