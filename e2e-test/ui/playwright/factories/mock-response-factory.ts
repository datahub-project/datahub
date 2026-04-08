/**
 * Re-exports from factories/mock-responses/ for backward compatibility.
 *
 * Prefer importing directly from the sub-modules:
 *   import { createSearchResponse } from './mock-responses/search';
 *
 * The class-style API below is kept for existing callers.
 */

export type { GraphQLResponse, GraphQLErrorResponse } from './mock-responses/common';
export {
  createErrorResponse,
  createSuccessResponse,
} from './mock-responses/common';
export { createSearchResponse } from './mock-responses/search';
export { createDatasetResponse } from './mock-responses/dataset';

import { createSearchResponse } from './mock-responses/search';
import { createDatasetResponse } from './mock-responses/dataset';
import { createErrorResponse, createSuccessResponse } from './mock-responses/common';
import type { GraphQLResponse, GraphQLErrorResponse } from './mock-responses/common';

/** @deprecated Use the named function exports from factories/mock-responses/ instead. */
export class MockResponseFactory {
  static createSearchResponse(
    entities: Record<string, unknown>[],
    total: number,
  ): GraphQLResponse {
    return createSearchResponse(entities, total);
  }

  static createDatasetResponse(dataset: Record<string, unknown>): GraphQLResponse {
    return createDatasetResponse(dataset);
  }

  static createErrorResponse(
    message: string,
    code: string = 'INTERNAL_SERVER_ERROR',
  ): GraphQLErrorResponse {
    return createErrorResponse(message, code);
  }

  static createSuccessResponse(data: Record<string, unknown>): GraphQLResponse {
    return createSuccessResponse(data);
  }
}
