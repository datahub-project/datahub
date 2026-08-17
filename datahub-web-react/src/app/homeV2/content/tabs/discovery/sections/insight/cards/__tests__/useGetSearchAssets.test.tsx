import { renderHook } from '@testing-library/react-hooks';
import { Mock, beforeEach, describe, expect, it, vi } from 'vitest';

import {
    INSIGHT_CARD_DISPLAY_COUNT,
    INSIGHT_CARD_FETCH_COUNT,
    useGetSearchAssets,
} from '@app/homeV2/content/tabs/discovery/sections/insight/cards/useGetSearchAssets';

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
                    count: INSIGHT_CARD_FETCH_COUNT,
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

    it('requests only the display count when separate siblings are enabled', () => {
        isShowSeparateSiblingsEnabled.mockReturnValue(true);

        renderHook(() => useGetSearchAssets([EntityType.Dataset], 'customers'));

        expect(queryMock).toHaveBeenCalledWith(
            expect.objectContaining({
                variables: expect.objectContaining({
                    input: expect.objectContaining({ count: INSIGHT_CARD_DISPLAY_COUNT }),
                }),
            }),
        );
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

    it('still returns five cards after collapsing a sibling pair in an over-fetched page', () => {
        const siblingPair = (primaryUrn: string, secondaryUrn: string) => [
            {
                urn: secondaryUrn,
                type: EntityType.Dataset,
                siblings: { isPrimary: false, siblings: [{ urn: primaryUrn }] },
            },
            {
                urn: primaryUrn,
                type: EntityType.Dataset,
                siblings: { isPrimary: true, siblings: [{ urn: secondaryUrn }] },
            },
        ];
        const unique = (urn: string) => ({ urn, type: EntityType.Dataset });
        const searchResults = [
            ...siblingPair('urn:li:dataset:primary-1', 'urn:li:dataset:secondary-1'),
            unique('urn:li:dataset:3'),
            unique('urn:li:dataset:4'),
            unique('urn:li:dataset:5'),
            unique('urn:li:dataset:6'),
        ].map((entity) => ({ entity }));

        queryMock.mockReturnValue({
            loading: false,
            data: { searchAcrossEntities: { searchResults } },
        });

        const { result } = renderHook(() => useGetSearchAssets([EntityType.Dataset]));

        expect(result.current.assets).toHaveLength(5);
        expect(result.current.assets.map((entity) => entity.urn)).toEqual([
            'urn:li:dataset:primary-1',
            'urn:li:dataset:3',
            'urn:li:dataset:4',
            'urn:li:dataset:5',
            'urn:li:dataset:6',
        ]);
    });
});
