import { describe, expect, it } from 'vitest';

import { redactUrl } from '@src/otel';

describe('redactUrl', () => {
    it('strips the query string (which can carry tokens/filters)', () => {
        expect(redactUrl('https://acme.io/api/v2/graphql?token=secret&q=pii')).toBe('https://acme.io/api/v2/graphql');
    });

    it('strips the fragment', () => {
        expect(redactUrl('https://acme.io/search#section')).toBe('https://acme.io/search');
    });

    it('resolves relative URLs against the current origin', () => {
        expect(redactUrl('/api/v2/graphql?x=1')).toBe(`${window.location.origin}/api/v2/graphql`);
    });

    it('leaves an already-clean URL unchanged', () => {
        expect(redactUrl('https://acme.io/datasets/urn')).toBe('https://acme.io/datasets/urn');
    });
});
