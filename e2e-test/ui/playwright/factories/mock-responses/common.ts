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

/** Build a generic GraphQL error response. */
export function createErrorResponse(message: string, code: string = 'INTERNAL_SERVER_ERROR'): GraphQLErrorResponse {
  return {
    status: 200,
    body: { errors: [{ message, extensions: { code } }] },
  };
}

/** Build a generic GraphQL success response wrapping arbitrary data. */
export function createSuccessResponse(data: Record<string, unknown>): GraphQLResponse {
  return { status: 200, body: { data } };
}
