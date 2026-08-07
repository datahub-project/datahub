import { renderHook } from '@testing-library/react-hooks';
import { Mock, beforeEach, describe, expect, it, vi } from 'vitest';

import { useGetSearchAssets } from '@app/homeV2/content/tabs/discovery/sections/insight/cards/useGetSearchAssets';

import { useGetSearchResultsForMultipleCardsQuery } from '@graphql/search.generated';
import { EntityType } from '@types';

vi.mock('@graphql/search.generated', () => ({
    useGetSearchResultsForMultipleCardsQuery: vi.fn(),
}));

vi.mock('@src/app/useAppConfig', () => ({
    useIsShowSeparateSiblingsEnabled: () => false,
}));

vi.mock('@src/app/search/utils/combineSiblingsInSearchResults', () => ({
    combineSiblingsInSearchResults: (_showSeparate: boolean, results: unknown) => results || [],
}));

describe('useGetSearchAssets', () => {
    const queryMock = useGetSearchResultsForMultipleCardsQuery as unknown as Mock;

    beforeEach(() => {
        vi.clearAllMocks();
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
});
