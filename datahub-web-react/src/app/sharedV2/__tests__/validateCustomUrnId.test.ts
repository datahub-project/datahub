import { describe, expect, it } from 'vitest';

import { validateCustomUrnId } from '@app/sharedV2/textUtil';

describe('validateCustomUrnId', () => {
    it('accepts ids without urn-reserved characters', () => {
        expect(validateCustomUrnId('my_term-1.0')).toBe(true);
    });

    it('rejects urn-reserved characters anywhere, including the first position', () => {
        [' a', ',a', '(a', ')a', ':a', 'a:b', 'a b'].forEach((id) => expect(validateCustomUrnId(id)).toBe(false));
    });
});
