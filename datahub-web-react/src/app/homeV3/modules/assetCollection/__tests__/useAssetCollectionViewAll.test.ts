import { renderHook } from '@testing-library/react-hooks';
import { useHistory } from 'react-router';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import useAssetCollectionViewAll, {
    convertLogicalPredicateToViewAllParams,
} from '@app/homeV3/modules/assetCollection/useAssetCollectionViewAll';
import { ENTITY_SUB_TYPE_FILTER_NAME, UnionType } from '@app/searchV2/utils/constants';
import { navigateToSearchUrl } from '@app/searchV2/utils/navigateToSearchUrl';
import { LogicalOperatorType, LogicalPredicate } from '@app/sharedV2/queryBuilder/builder/types';

import { DataHubPageModuleType, EntityType, FilterOperator, PageModuleScope } from '@types';

vi.mock('react-router', () => ({
    useHistory: vi.fn(),
}));

vi.mock('@app/searchV2/utils/navigateToSearchUrl', () => ({
    navigateToSearchUrl: vi.fn(),
}));

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistryV2: () => ({
        getTypeFromGraphName: (name: string) =>
            (({ dataProduct: EntityType.DataProduct }) as Record<string, EntityType>)[name],
    }),
}));

const getTypeFromGraphName = (name: string): EntityType | undefined =>
    (({ dataProduct: EntityType.DataProduct, dataset: EntityType.Dataset }) as Record<string, EntityType>)[name];

const predicate = (operator: LogicalOperatorType, operands: LogicalPredicate['operands']): LogicalPredicate => ({
    type: 'logical',
    operator,
    operands,
});

const DATA_PRODUCT_FILTER_JSON = JSON.stringify({
    type: 'logical',
    operator: 'and',
    operands: [{ type: 'property', property: '_entityType', operator: 'equals', values: ['dataProduct'] }],
});

function buildModule(assetCollectionParams?: {
    assetUrns?: string[];
    dynamicFilterJson?: string;
}): Parameters<typeof useAssetCollectionViewAll>[0] {
    return {
        urn: 'urn:li:dataHubPageModule:test',
        type: EntityType.DatahubPageModule,
        properties: {
            name: 'Test Collection',
            type: DataHubPageModuleType.AssetCollection,
            visibility: { scope: PageModuleScope.Personal },
            params: assetCollectionParams ? { assetCollectionParams: { assetUrns: [], ...assetCollectionParams } } : {},
        },
    } as Parameters<typeof useAssetCollectionViewAll>[0];
}

describe('convertLogicalPredicateToViewAllParams', () => {
    it('converts a flat AND of entity type and tags', () => {
        const result = convertLogicalPredicateToViewAllParams(
            predicate(LogicalOperatorType.AND, [
                { type: 'property', property: '_entityType', operator: 'equals', values: ['dataProduct'] },
                { type: 'property', property: 'tags', operator: 'equals', values: ['urn:li:tag:golden'] },
            ]),
            getTypeFromGraphName,
        );
        expect(result).toEqual({
            unionType: UnionType.AND,
            filters: [
                { field: ENTITY_SUB_TYPE_FILTER_NAME, values: ['DATA_PRODUCT'], condition: FilterOperator.Equal },
                { field: 'tags', values: ['urn:li:tag:golden'], condition: FilterOperator.Equal },
            ],
        });
    });

    it('converts a flat OR with unionType OR', () => {
        const result = convertLogicalPredicateToViewAllParams(
            predicate(LogicalOperatorType.OR, [
                { type: 'property', property: 'tags', operator: 'equals', values: ['urn:li:tag:a'] },
                { type: 'property', property: 'owners', operator: 'equals', values: ['urn:li:corpuser:b'] },
            ]),
            getTypeFromGraphName,
        );
        expect(result?.unionType).toEqual(UnionType.OR);
        expect(result?.filters).toHaveLength(2);
    });

    it('skips blank builder rows but keeps valid leaves', () => {
        const result = convertLogicalPredicateToViewAllParams(
            predicate(LogicalOperatorType.AND, [
                { type: 'property' },
                { type: 'property', property: '_entityType', operator: 'equals', values: ['dataset'] },
            ]),
            getTypeFromGraphName,
        );
        expect(result?.filters).toEqual([
            { field: ENTITY_SUB_TYPE_FILTER_NAME, values: ['DATASET'], condition: FilterOperator.Equal },
        ]);
    });

    it('returns null for nested groups', () => {
        const result = convertLogicalPredicateToViewAllParams(
            predicate(LogicalOperatorType.OR, [
                predicate(LogicalOperatorType.AND, [
                    { type: 'property', property: 'tags', operator: 'equals', values: ['urn:li:tag:a'] },
                ]),
            ]),
            getTypeFromGraphName,
        );
        expect(result).toBeNull();
    });

    it('returns null for NOT predicates', () => {
        const result = convertLogicalPredicateToViewAllParams(
            predicate(LogicalOperatorType.NOT, [
                { type: 'property', property: 'tags', operator: 'equals', values: ['urn:li:tag:a'] },
            ]),
            getTypeFromGraphName,
        );
        expect(result).toBeNull();
    });

    it('returns null for exists operator', () => {
        const result = convertLogicalPredicateToViewAllParams(
            predicate(LogicalOperatorType.AND, [{ type: 'property', property: 'tags', operator: 'exists' }]),
            getTypeFromGraphName,
        );
        expect(result).toBeNull();
    });

    it('returns null when an exists leaf appears alongside convertible leaves (must not be silently dropped)', () => {
        const result = convertLogicalPredicateToViewAllParams(
            predicate(LogicalOperatorType.AND, [
                { type: 'property', property: '_entityType', operator: 'equals', values: ['dataProduct'] },
                { type: 'property', property: 'tags', operator: 'exists' },
            ]),
            getTypeFromGraphName,
        );
        expect(result).toBeNull();
    });

    it('returns null for unknown operators', () => {
        const result = convertLogicalPredicateToViewAllParams(
            predicate(LogicalOperatorType.AND, [
                { type: 'property', property: 'tags', operator: 'soundsLike', values: ['x'] },
            ]),
            getTypeFromGraphName,
        );
        expect(result).toBeNull();
    });

    it('returns null for unmappable entity type values', () => {
        const result = convertLogicalPredicateToViewAllParams(
            predicate(LogicalOperatorType.AND, [
                { type: 'property', property: '_entityType', operator: 'equals', values: ['notARealType'] },
            ]),
            getTypeFromGraphName,
        );
        expect(result).toBeNull();
    });

    it('returns null when no usable leaves remain', () => {
        const result = convertLogicalPredicateToViewAllParams(
            predicate(LogicalOperatorType.AND, [
                { type: 'property' },
                { type: 'property', property: 'tags', operator: 'equals', values: [] },
            ]),
            getTypeFromGraphName,
        );
        expect(result).toBeNull();
    });

    it('returns null for AND with two entity-type leaves (tile ANDs into one clause, search page would OR them)', () => {
        const result = convertLogicalPredicateToViewAllParams(
            predicate(LogicalOperatorType.AND, [
                { type: 'property', property: '_entityType', operator: 'equals', values: ['dataset'] },
                { type: 'property', property: '_entityType', operator: 'equals', values: ['dataProduct'] },
            ]),
            getTypeFromGraphName,
        );
        expect(result).toBeNull();
    });

    it('allows OR with two entity-type leaves', () => {
        const result = convertLogicalPredicateToViewAllParams(
            predicate(LogicalOperatorType.OR, [
                { type: 'property', property: '_entityType', operator: 'equals', values: ['dataset'] },
                { type: 'property', property: '_entityType', operator: 'equals', values: ['dataProduct'] },
            ]),
            getTypeFromGraphName,
        );
        expect(result).not.toBeNull();
        expect(result?.filters).toHaveLength(2);
    });

    it('returns null for AND with a valid tags leaf plus an owners leaf with empty values', () => {
        const result = convertLogicalPredicateToViewAllParams(
            predicate(LogicalOperatorType.AND, [
                { type: 'property', property: 'tags', operator: 'equals', values: ['urn:li:tag:golden'] },
                { type: 'property', property: 'owners', operator: 'equals', values: [] },
            ]),
            getTypeFromGraphName,
        );
        expect(result).toBeNull();
    });
});

describe('useAssetCollectionViewAll', () => {
    const mockHistory = { push: vi.fn() };

    beforeEach(() => {
        vi.clearAllMocks();
        (useHistory as ReturnType<typeof vi.fn>).mockReturnValue(mockHistory);
    });

    it('returns a callback that navigates to search for a flat dynamic filter', () => {
        const { result } = renderHook(() =>
            useAssetCollectionViewAll(buildModule({ dynamicFilterJson: DATA_PRODUCT_FILTER_JSON })),
        );
        expect(result.current).toBeDefined();
        result.current?.();
        expect(navigateToSearchUrl).toHaveBeenCalledWith({
            history: mockHistory,
            query: '*',
            unionType: UnionType.AND,
            filters: [
                { field: ENTITY_SUB_TYPE_FILTER_NAME, values: ['DATA_PRODUCT'], condition: FilterOperator.Equal },
            ],
        });
    });

    it('returns undefined for manual urn collections', () => {
        const { result } = renderHook(() =>
            useAssetCollectionViewAll(
                buildModule({ assetUrns: ['urn:li:dataset:1'], dynamicFilterJson: DATA_PRODUCT_FILTER_JSON }),
            ),
        );
        expect(result.current).toBeUndefined();
    });

    it('returns undefined when there is no dynamic filter', () => {
        const { result } = renderHook(() => useAssetCollectionViewAll(buildModule()));
        expect(result.current).toBeUndefined();
    });

    it('returns undefined for malformed dynamic filter JSON', () => {
        const { result } = renderHook(() => useAssetCollectionViewAll(buildModule({ dynamicFilterJson: '{not json' })));
        expect(result.current).toBeUndefined();
    });

    it('returns undefined for unrepresentable predicates', () => {
        const nested = JSON.stringify({
            type: 'logical',
            operator: 'or',
            operands: [
                {
                    type: 'logical',
                    operator: 'and',
                    operands: [{ type: 'property', property: 'tags', operator: 'equals', values: ['urn:li:tag:a'] }],
                },
            ],
        });
        const { result } = renderHook(() => useAssetCollectionViewAll(buildModule({ dynamicFilterJson: nested })));
        expect(result.current).toBeUndefined();
    });
});
