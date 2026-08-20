import { describe, expect, it } from 'vitest';

import { sortNativeResults } from '@app/entityV2/shared/tabs/Dataset/Validations/assertionUtils';

describe('sortNativeResults', () => {
    it('sorts keys deterministically without mutating the input', () => {
        const entries = [
            { key: 'value10', value: 'ten' },
            { key: 'Value2', value: 'two' },
            { key: 'actual', value: 'one' },
        ];

        expect(sortNativeResults(entries)).toEqual([
            { key: 'actual', value: 'one' },
            { key: 'Value2', value: 'two' },
            { key: 'value10', value: 'ten' },
        ]);
        expect(entries[0].key).toBe('value10');
    });
});
