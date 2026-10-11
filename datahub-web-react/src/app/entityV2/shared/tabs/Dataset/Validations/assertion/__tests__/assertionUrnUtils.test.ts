import { describe, expect, it } from 'vitest';

import { isValidAssertionUrnFormat } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/assertionUrnUtils';

describe('isValidAssertionUrnFormat', () => {
    it('accepts assertion URNs', () => {
        expect(isValidAssertionUrnFormat('urn:li:assertion:test')).toBe(true);
    });

    it.each([
        'urn:li:assertion:test?tab=summary',
        'urn:li:assertion:test#details',
        'urn:li:assertion:test value',
        'https://example.com/assertion',
    ])('rejects malformed assertion value %s', (value) => {
        expect(isValidAssertionUrnFormat(value)).toBe(false);
    });
});
