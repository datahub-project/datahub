import { renderHook } from '@testing-library/react-hooks';
import { Mock, beforeEach, describe, expect, it, vi } from 'vitest';

import { useGetSearchAssets } from '@app/homeV2/content/tabs/discovery/sections/insight/cards/useGetSearchAssets';
import { UnionType } from '@app/searchV2/utils/constants';

import { useGetSearchResultsForMultipleCardsQuery } from '@graphql/search.generated';
import { EntityType } from '@types';

vi.mock('@graphql/search.generated', () => ({
    useGetSearchResultsForMultipleCardsQuery: vi.fn(),
}));

vi.mock('@src/app/useAppConfig', () => ({
    useIsShowSeparateSiblingsEnabled: () => false,
}));

vi.mock('@src/app/search/utils/combineSiblingsInSearchResults', () => ({
    combineSiblingsInSearchResults: (_separate: boolean, results: Array<{ entity: { urn: string } }> | undefined) =>
        results || [],
}));

describe('useGetSearchAssets', () => {
    const queryMock = useGetSearchResultsForMultipleCardsQuery as unknown as Mock;

    beforeEach(() => {
        vi.clearAllMocks();
        queryMock.mockReturnValue({
            loading: false,
            data: {
                searchAcrossEntities: {
                    searchResults: [{ entity: { urn: 'urn:li:dataset:1', type: EntityType.Dataset } }],
                },
            },
        });
    });

    it('uses the trimmed cards query with count 5 and skipAggregates', () => {
        renderHook(() =>
            useGetSearchAssets(
                [EntityType.Dataset],
                'orders',
                { unionType: UnionType.AND, filters: [{ field: 'tags', values: ['urn:li:tag:pii'] }] },
                undefined,
                'urn:li:dataHubView:default',
            ),
        );

        expect(queryMock).toHaveBeenCalledWith({
            variables: {
                input: {
                    types: [EntityType.Dataset],
                    query: 'orders',
                    start: 0,
                    count: 5,
                    orFilters: [{ and: [{ field: 'tags', values: ['urn:li:tag:pii'] }] }],
                    sortInput: null,
                    viewUrn: 'urn:li:dataHubView:default',
                    searchFlags: { skipAggregates: true },
                },
            },
            fetchPolicy: 'cache-first',
        });
    });

    it('returns mapped assets from search results', () => {
        const { result } = renderHook(() => useGetSearchAssets());
        expect(result.current.assets).toEqual([{ urn: 'urn:li:dataset:1', type: EntityType.Dataset }]);
        expect(result.current.loading).toBe(false);
    });
});
