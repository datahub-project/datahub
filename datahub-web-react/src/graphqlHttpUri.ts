import { resolveRuntimePath } from '@utils/runtimeBasePath';

/**
 * Apollo HttpLink URI builder. Appends the GraphQL operation name as a query param so Chrome
 * DevTools Network shows distinct rows (and supports filter-by-name) instead of dozens of identical
 * `graphql` POSTs. GMS ignores the query string and still reads `operationName` from the body;
 * OpenTelemetry fetch instrumentation redacts query strings from span URLs.
 */
export function buildGraphqlHttpUri(operationName?: string | null): string {
    const name = operationName || 'anonymous';
    return `${resolveRuntimePath('/api/v2/graphql')}?operationName=${encodeURIComponent(name)}`;
}
