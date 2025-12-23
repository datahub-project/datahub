import { getRedirectUrl } from '@conf/utils';

describe('getRedirectUrl', () => {
    it('should replace old routes while preserving query string exactly once', () => {
        const newRoutes = {
            '/Validation/Assertions': '/Quality/List',
        };

        const location = {
            pathname:
                '/dataset/urn%3Ali%3Adataset%3A(urn%3Ali%3AdataPlatform%3Asnowflake%2Cfoo%2CPROD)/Validation/Assertions',
            search: '?assertion_urn=urn%3Ali%3Aassertion%3Add29db5a-9ade-4694-952d-d98c8206e8f9',
        } as any;

        expect(getRedirectUrl(newRoutes, location)).toEqual(
            '/dataset/urn%3Ali%3Adataset%3A(urn%3Ali%3AdataPlatform%3Asnowflake%2Cfoo%2CPROD)/Quality/List?assertion_urn=urn%3Ali%3Aassertion%3Add29db5a-9ade-4694-952d-d98c8206e8f9',
        );
    });

    it('should return the original path and query when no routes are provided', () => {
        const location = {
            pathname:
                '/dataset/urn%3Ali%3Adataset%3A(urn%3Ali%3AdataPlatform%3Asnowflake%2Cfoo%2CPROD)/Validation/Assertions',
            search: '?assertion_urn=urn%3Ali%3Aassertion%3Atest',
        } as any;

        expect(getRedirectUrl(undefined as any, location)).toEqual(
            '/dataset/urn%3Ali%3Adataset%3A(urn%3Ali%3AdataPlatform%3Asnowflake%2Cfoo%2CPROD)/Validation/Assertions?assertion_urn=urn%3Ali%3Aassertion%3Atest',
        );
    });
});
