import { DataProductEntity } from '@app/marketplace/marketplaceTypes';
import {
    mergeMarketplaceVisibleRootProducts,
    resolveMarketplaceFallbackRootUrn,
} from '@app/marketplace/utils/marketplaceSidebarBrowse';

import { EntityType } from '@types';

describe('marketplaceSidebarBrowse', () => {
    const roots = [{ urn: 'urn:li:dataProduct:root-a' }, { urn: 'urn:li:dataProduct:root-b' }];

    describe('resolveMarketplaceFallbackRootUrn', () => {
        it('returns null in search mode', () => {
            expect(
                resolveMarketplaceFallbackRootUrn(
                    { entityType: EntityType.DataProduct, urn: 'urn:li:dataProduct:child' },
                    roots,
                    true,
                ),
            ).toBeNull();
        });

        it('returns missing root ancestor when nested selection is open', () => {
            expect(
                resolveMarketplaceFallbackRootUrn(
                    {
                        entityType: EntityType.DataProduct,
                        urn: 'urn:li:dataProduct:child',
                        parentDataProducts: [{ urn: 'urn:li:dataProduct:root-missing' }],
                    },
                    roots,
                    false,
                ),
            ).toBe('urn:li:dataProduct:root-missing');
        });

        it('returns selected root urn when it is missing from the loaded list', () => {
            expect(
                resolveMarketplaceFallbackRootUrn(
                    {
                        entityType: EntityType.DataProduct,
                        urn: 'urn:li:dataProduct:root-missing',
                        parentDataProducts: [],
                    },
                    roots,
                    false,
                ),
            ).toBe('urn:li:dataProduct:root-missing');
        });

        it('returns null when selected root is already loaded', () => {
            expect(
                resolveMarketplaceFallbackRootUrn(
                    {
                        entityType: EntityType.DataProduct,
                        urn: 'urn:li:dataProduct:root-a',
                        parentDataProducts: [],
                    },
                    roots,
                    false,
                ),
            ).toBeNull();
        });
    });

    describe('mergeMarketplaceVisibleRootProducts', () => {
        it('returns existing roots when fallback is empty', () => {
            const merged = mergeMarketplaceVisibleRootProducts(roots as DataProductEntity[], []);
            expect(merged).toBe(roots);
        });

        it('prepends fallback products that are not already present', () => {
            const fallback = [{ urn: 'urn:li:dataProduct:root-missing' }] as DataProductEntity[];
            expect(mergeMarketplaceVisibleRootProducts(roots as DataProductEntity[], fallback)).toEqual([
                ...fallback,
                ...roots,
            ]);
        });

        it('does not duplicate roots already in the list', () => {
            const fallback = [{ urn: 'urn:li:dataProduct:root-a' }] as DataProductEntity[];
            expect(mergeMarketplaceVisibleRootProducts(roots as DataProductEntity[], fallback)).toBe(roots);
        });
    });
});
