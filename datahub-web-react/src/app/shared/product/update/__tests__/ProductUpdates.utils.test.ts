import {
    getDefaultProductUpdateLink,
    getLocalizedCurrentMonth,
    getProductUpdateVersion,
} from '@app/shared/product/update/ProductUpdates.utils';

describe('ProductUpdates utilities', () => {
    it('extracts a display version and builds the default Cloud release URL', () => {
        expect(getProductUpdateVersion('v2.2')).toBe('2.2');
        expect(getDefaultProductUpdateLink('v2.2', true)).toBe('https://datahub.com/blog/datahub-cloud-2-2');
    });

    it('does not generate a Cloud release URL for Core or unrecognized update ids', () => {
        expect(getDefaultProductUpdateLink('v2.2')).toBeNull();
        expect(getDefaultProductUpdateLink('v2.2', false)).toBeNull();
        expect(getProductUpdateVersion('september-update')).toBeNull();
        expect(getDefaultProductUpdateLink('september-update', true)).toBeNull();
    });

    it('formats the current month for the requested locale', () => {
        const september = new Date('2026-09-04T00:00:00Z');

        expect(getLocalizedCurrentMonth('en', september)).toBe('September');
        expect(getLocalizedCurrentMonth('ja', september)).toBe('9月');
    });
});
