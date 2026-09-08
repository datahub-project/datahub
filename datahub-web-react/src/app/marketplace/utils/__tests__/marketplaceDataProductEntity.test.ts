import {
    countPendingOptimistic,
    isRootDataProduct,
    mergeDataProductEntities,
    pruneOptimisticProducts,
    toMarketplaceDataProductEntity,
} from '@app/marketplace/utils/marketplaceDataProductEntity';
import { mergeScrollPageResults } from '@app/marketplace/utils/scrollMergeUtils';

describe('marketplaceDataProductEntity', () => {
    const root = (id: string) =>
        ({
            __typename: 'DataProduct',
            urn: `urn:li:dataProduct:${id}`,
            type: 'DATA_PRODUCT',
            properties: { name: id },
        }) as const;

    describe('mergeDataProductEntities', () => {
        it('prepends optimistic rows missing from fetched results', () => {
            const merged = mergeDataProductEntities([root('a') as any], [root('b') as any]);
            expect(merged.map((p) => p.urn)).toEqual(['urn:li:dataProduct:b', 'urn:li:dataProduct:a']);
        });
    });

    describe('countPendingOptimistic', () => {
        it('counts only optimistic rows not yet indexed', () => {
            expect(countPendingOptimistic([root('a') as any], [root('a') as any, root('b') as any])).toBe(1);
        });
    });

    describe('pruneOptimisticProducts', () => {
        it('removes optimistic rows once indexed', () => {
            const optimistic = [root('a') as any, root('b') as any];
            expect(pruneOptimisticProducts(optimistic, ['urn:li:dataProduct:a']).map((p) => p.urn)).toEqual([
                'urn:li:dataProduct:b',
            ]);
        });
    });

    describe('isRootDataProduct', () => {
        it('returns true when parent is absent', () => {
            expect(isRootDataProduct({ properties: {} } as any)).toBe(true);
        });
    });

    describe('toMarketplaceDataProductEntity', () => {
        it('maps create mutation payload into browse entity shape', () => {
            const entity = toMarketplaceDataProductEntity({
                urn: 'urn:li:dataProduct:new',
                type: 'DATA_PRODUCT',
                properties: { name: 'New' },
            } as any);
            expect(entity.urn).toBe('urn:li:dataProduct:new');
            expect(entity.childDataProducts?.total).toBe(0);
        });
    });
});

describe('scrollMergeUtils', () => {
    it('replaces current list on first page', () => {
        expect(
            mergeScrollPageResults({
                current: [{ urn: 'old' }],
                fresh: [{ urn: 'new' }],
                scrollId: null,
            }).map((e) => e.urn),
        ).toEqual(['new']);
    });

    it('appends unseen rows on later pages', () => {
        expect(
            mergeScrollPageResults({
                current: [{ urn: 'a' }],
                fresh: [{ urn: 'b' }],
                scrollId: 'page-2',
            }).map((e) => e.urn),
        ).toEqual(['a', 'b']);
    });
});
