import {
    PRODUCT_UPDATE_BOOTSTRAP_KEY,
    bundledProductUpdateImage,
    getDefaultProductUpdateLink,
    getLocalizedReleaseMonth,
    getProductUpdateVersion,
    isProductUpdateHiddenByDismiss,
    readProductUpdateBootstrap,
    resolveProductUpdateImage,
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

    it('uses the image shipped in this frontend', () => {
        expect(bundledProductUpdateImage(true)).toContain('in-product-release-oct2026.png');
        expect(bundledProductUpdateImage(false)).toContain('sample-product-update-image.png');
    });

    it('reads an enabled product update injected into the page and ignores a disabled one', () => {
        window[PRODUCT_UPDATE_BOOTSTRAP_KEY] = {
            enabled: true,
            id: 'v2.3',
            image: 'https://example.com/release.png',
        };
        expect(readProductUpdateBootstrap()).toMatchObject({ id: 'v2.3', image: 'https://example.com/release.png' });

        window[PRODUCT_UPDATE_BOOTSTRAP_KEY] = { enabled: false, id: 'v2.3' };
        expect(readProductUpdateBootstrap()).toBeNull();
        delete window[PRODUCT_UPDATE_BOOTSTRAP_KEY];
    });

    it('prefers the hosted image URL and falls back to the bundled file', () => {
        expect(resolveProductUpdateImage('https://example.com/release.png', true)).toBe(
            'https://example.com/release.png',
        );
        expect(resolveProductUpdateImage(undefined, true)).toContain('in-product-release-oct2026.png');
        expect(resolveProductUpdateImage('null', false)).toContain('sample-product-update-image.png');
    });

    it('hides only the announcement id that was dismissed', () => {
        localStorage.setItem('dismissedProductUpdates', JSON.stringify(['v2.2']));
        expect(isProductUpdateHiddenByDismiss('v2.2', null)).toBe(true);
        expect(isProductUpdateHiddenByDismiss('v2.3', null)).toBe(false);
        expect(isProductUpdateHiddenByDismiss('v2.3', 'v2.2')).toBe(false);
        expect(isProductUpdateHiddenByDismiss('v2.3', 'v2.3')).toBe(true);
        localStorage.clear();
    });

    it('does not infer a month when the release month is absent or invalid', () => {
        expect(getLocalizedReleaseMonth('en', undefined)).toBeNull();
        expect(getLocalizedReleaseMonth('en', '2026-13')).toBeNull();
        expect(getLocalizedReleaseMonth('en', 'September 2026')).toBeNull();
    });
});
