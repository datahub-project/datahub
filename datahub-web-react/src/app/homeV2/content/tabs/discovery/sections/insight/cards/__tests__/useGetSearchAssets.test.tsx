import { renderHook } from '@testing-library/react-hooks';
import { Mock, beforeEach, describe, expect, it, vi } from 'vitest';

import { useGetSearchAssets } from '@app/homeV2/content/tabs/discovery/sections/insight/cards/useGetSearchAssets';

import { useGetSearchResultsForMultipleCardsQuery } from '@graphql/search.generated';
import { EntityType } from '@types';

vi.mock('@graphql/search.generated', () => ({
    useGetSearchResultsForMultipleCardsQuery: vi.fn(),
}));

const { isShowSeparateSiblingsEnabled } = vi.hoisted(() => ({
    isShowSeparateSiblingsEnabled: vi.fn(() => false),
}));

vi.mock('@src/app/useAppConfig', () => ({
    useIsShowSeparateSiblingsEnabled: isShowSeparateSiblingsEnabled,
}));

describe('useGetSearchAssets', () => {
    const queryMock = useGetSearchResultsForMultipleCardsQuery as unknown as Mock;

    beforeEach(() => {
        vi.clearAllMocks();
        isShowSeparateSiblingsEnabled.mockReturnValue(false);
        queryMock.mockReturnValue({
            loading: false,
            data: undefined,
        });
    });

    it('uses the cards search query with cache-first policy', () => {
        renderHook(() => useGetSearchAssets([EntityType.Dataset], 'customers'));

        expect(queryMock).toHaveBeenCalledWith({
            variables: {
                input: {
                    types: [EntityType.Dataset],
                    query: 'customers',
                    start: 0,
                    count: 5,
                    orFilters: null,
                    sortInput: null,
                    viewUrn: undefined,
                    searchFlags: {
                        skipAggregates: true,
                    },
                },
            },
            fetchPolicy: 'cache-first',
        });
    });

    it('returns assets from search results', () => {
        const entity = { urn: 'urn:li:dataset:1', type: EntityType.Dataset };
        queryMock.mockReturnValue({
            loading: false,
            data: {
                searchAcrossEntities: {
                    searchResults: [{ entity }],
                },
            },
        });

        const { result } = renderHook(() => useGetSearchAssets([EntityType.Dataset]));

        expect(result.current.assets).toEqual([entity]);
        expect(result.current.loading).toBe(false);
    });

    it('renders one asset per sibling cohort, keeping the primary sibling', () => {
        const dbt = {
            urn: 'urn:li:dataset:(urn:li:dataPlatform:dbt,my_db.my_schema.events,PROD)',
            type: EntityType.Dataset,
            siblings: { isPrimary: false, siblings: [{ urn: 'urn:li:dataset:warehouse' }] },
        };
        const warehouse = {
            urn: 'urn:li:dataset:warehouse',
            type: EntityType.Dataset,
            siblings: { isPrimary: true, siblings: [{ urn: dbt.urn }] },
        };
        queryMock.mockReturnValue({
            loading: false,
            data: { searchAcrossEntities: { searchResults: [{ entity: dbt }, { entity: warehouse }] } },
        });

        const { result } = renderHook(() => useGetSearchAssets([EntityType.Dataset]));

        expect(result.current.assets).toEqual([warehouse]);
    });

    it('keeps siblings separate when the separate-siblings flag is on', () => {
        isShowSeparateSiblingsEnabled.mockReturnValue(true);
        const dbt = {
            urn: 'urn:li:dataset:(urn:li:dataPlatform:dbt,my_db.my_schema.events,PROD)',
            type: EntityType.Dataset,
            siblings: { isPrimary: false, siblings: [{ urn: 'urn:li:dataset:warehouse' }] },
        };
        const warehouse = {
            urn: 'urn:li:dataset:warehouse',
            type: EntityType.Dataset,
            siblings: { isPrimary: true, siblings: [{ urn: dbt.urn }] },
        };
        queryMock.mockReturnValue({
            loading: false,
            data: { searchAcrossEntities: { searchResults: [{ entity: dbt }, { entity: warehouse }] } },
        });

        const { result } = renderHook(() => useGetSearchAssets([EntityType.Dataset]));

        expect(result.current.assets).toEqual([dbt, warehouse]);
    });
});
