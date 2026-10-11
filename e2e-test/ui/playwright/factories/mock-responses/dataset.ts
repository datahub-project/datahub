import type { GraphQLResponse } from './common';

/** Build a `dataset` GraphQL response. */
export function createDatasetResponse(dataset: Record<string, unknown>): GraphQLResponse {
  return { status: 200, body: { data: { dataset } } };
}
