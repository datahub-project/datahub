import { describe, expect, it } from 'vitest';

import { buildGraphqlHttpUri } from '@src/graphqlHttpUri';

describe('buildGraphqlHttpUri', () => {
    it('appends operationName for Network tab filtering', () => {
        expect(buildGraphqlHttpUri('appConfig')).toBe('/api/v2/graphql?operationName=appConfig');
    });

    it('falls back to anonymous when operation name is missing', () => {
        expect(buildGraphqlHttpUri(undefined)).toBe('/api/v2/graphql?operationName=anonymous');
        expect(buildGraphqlHttpUri(null)).toBe('/api/v2/graphql?operationName=anonymous');
        expect(buildGraphqlHttpUri('')).toBe('/api/v2/graphql?operationName=anonymous');
    });

    it('URL-encodes operation names', () => {
        expect(buildGraphqlHttpUri('get Dataset')).toBe('/api/v2/graphql?operationName=get%20Dataset');
    });
});
