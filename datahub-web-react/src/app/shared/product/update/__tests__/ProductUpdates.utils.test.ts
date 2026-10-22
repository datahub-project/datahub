import {
    getDefaultProductUpdateLink,
    getLocalizedReleaseMonth,
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

    it('formats the configured release month for the requested locale', () => {
        expect(getLocalizedReleaseMonth('en', '2026-09')).toBe('September');
        expect(getLocalizedReleaseMonth('ja', '2026-09')).toBe('9月');
    });

    it('does not infer a month when the release month is absent or invalid', () => {
        expect(getLocalizedReleaseMonth('en', undefined)).toBeNull();
        expect(getLocalizedReleaseMonth('en', '2026-13')).toBeNull();
        expect(getLocalizedReleaseMonth('en', 'September 2026')).toBeNull();
    });
});
