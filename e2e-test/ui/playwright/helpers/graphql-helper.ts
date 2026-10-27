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

  /**
   * Resolve with the next response for `operationName`.
   *
   * Pass `matchVariables` when the page may already have the same operation in
   * flight (e.g. a list query that fires when a lazy route chunk renders). Without
   * it, the waiter can resolve on that earlier request instead of the one the test
   * action triggers.
   */
  async waitForGraphQLResponse(
    operationName: string,
    matchVariables?: (variables: Record<string, unknown>) => boolean,
  ): Promise<GraphQLResponse> {
    const response = await this.page.waitForResponse((r) => {
      if (!r.url().includes('/graphql')) {
        return false;
      }
      const body = r.request().postDataJSON() as { operationName?: string; variables?: unknown } | null;
      if (body?.operationName !== operationName) {
        return false;
      }
      if (!matchVariables) {
        return true;
      }
      const variables = body.variables;
      return (
        typeof variables === 'object' && variables !== null && matchVariables(variables as Record<string, unknown>)
      );
    });
    return response.json() as Promise<GraphQLResponse>;
  }
}
