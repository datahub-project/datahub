/** Shape of a GraphQL success response wrapper. */
export interface GraphQLResponse<T = Record<string, unknown>> {
  status: number;
  body: { data: T };
}

/** Shape of a GraphQL error response. */
export interface GraphQLErrorResponse {
  status: number;
  body: { errors: Array<{ message: string; extensions: { code: string } }> };
}

export class MockResponseFactory {
  static createSearchResponse(
    entities: Record<string, unknown>[],
    total: number,
  ): GraphQLResponse {
    return {
      status: 200,
      body: {
        data: {
          search: {
            searchResults: entities.map((entity) => ({
              entity,
              matchedFields: [],
            })),
            total,
          },
        },
      },
    };
  }

  static createDatasetResponse(dataset: Record<string, unknown>): GraphQLResponse {
    return {
      status: 200,
      body: { data: { dataset } },
    };
  }

  static createErrorResponse(
    message: string,
    code: string = 'INTERNAL_SERVER_ERROR',
  ): GraphQLErrorResponse {
    return {
      status: 200,
      body: {
        errors: [{ message, extensions: { code } }],
      },
    };
  }

  static createSuccessResponse(data: Record<string, unknown>): GraphQLResponse {
    return {
      status: 200,
      body: { data },
    };
  }
}
