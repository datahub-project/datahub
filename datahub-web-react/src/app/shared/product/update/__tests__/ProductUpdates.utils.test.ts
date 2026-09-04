import {
    getDefaultProductUpdateLink,
    getLocalizedCurrentMonth,
    getProductUpdateVersion,
} from '@app/shared/product/update/ProductUpdates.utils';

describe('ProductUpdates utilities', () => {
    it('extracts a display version and builds the default Cloud release URL', () => {
        expect(getProductUpdateVersion('v2.2')).toBe('2.2');
        expect(getDefaultProductUpdateLink('v2.2')).toBe('https://datahub.com/blog/datahub-cloud-2-2');
    });

    it('does not generate a release URL from an unrecognized update id', () => {
        expect(getProductUpdateVersion('september-update')).toBeNull();
        expect(getDefaultProductUpdateLink('september-update')).toBeNull();
    });

    it('formats the current month for the requested locale', () => {
        const september = new Date('2026-09-04T00:00:00Z');

        expect(getLocalizedCurrentMonth('en', september)).toBe('September');
        expect(getLocalizedCurrentMonth('ja', september)).toBe('9月');
    });
});
