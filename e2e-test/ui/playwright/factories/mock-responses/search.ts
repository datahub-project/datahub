import type { GraphQLResponse } from './common';

/** Build a `search` GraphQL response. */
export function createSearchResponse(
  entities: Record<string, unknown>[],
  total: number,
): GraphQLResponse {
  return {
    status: 200,
    body: {
      data: {
        search: {
          searchResults: entities.map((entity) => ({ entity, matchedFields: [] })),
          total,
        },
      },
    },
  };
}
