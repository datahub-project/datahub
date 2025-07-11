import { renderHook } from '@testing-library/react-hooks';
import { Mock, beforeEach, describe, expect, it, vi } from 'vitest';

import useGetAssetResults from '@app/homeV3/modules/assetCollection/useGetAssetResults';
import { convertFiltersMapToFilters } from '@app/searchV2/filtersV2/utils';
import { generateOrFilters } from '@app/searchV2/utils/generateOrFilters';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';

// Mock dependencies
vi.mock('@app/searchV2/filtersV2/utils', () => ({
    convertFiltersMapToFilters: vi.fn(),
}));

vi.mock('@app/searchV2/utils/generateOrFilters', () => ({
    generateOrFilters: vi.fn(),
}));

vi.mock('@graphql/search.generated', () => ({
    useGetSearchResultsForMultipleQuery: vi.fn(),
}));

describe('useGetAssetResults', () => {
    const mockAppliedFilters = new Map();
    const mockFilters = [{ field: 'testField', value: 'testValue' }];
    const mockOrFilters = [{ or: 'filters' }];
    const mockData = {
        searchAcrossEntities: {
            searchResults: [{ entity: { urn: 'urn:li:entity:1' } }, { entity: { urn: 'urn:li:entity:2' } }],
        },
    };

    const convertFiltersMapToFiltersMock = convertFiltersMapToFilters as unknown as Mock;
    const generateOrFiltersMock = generateOrFilters as unknown as Mock;
    const useGetSearchResultsForMultipleQueryMock = useGetSearchResultsForMultipleQuery as unknown as Mock;

    beforeEach(() => {
        vi.clearAllMocks();
        convertFiltersMapToFiltersMock.mockReturnValue(mockFilters);
        generateOrFiltersMock.mockReturnValue(mockOrFilters);
        useGetSearchResultsForMultipleQueryMock.mockReturnValue({ data: mockData });
    });

    it('should call convertFiltersMapToFilters with appliedFilters', () => {
        renderHook(() => useGetAssetResults({ searchQuery: 'query', appliedFilters: mockAppliedFilters }));
        expect(convertFiltersMapToFiltersMock).toHaveBeenCalledWith(mockAppliedFilters);
    });

    it('should call generateOrFilters with UnionType.AND and filters', () => {
        renderHook(() => useGetAssetResults({ searchQuery: 'query', appliedFilters: mockAppliedFilters }));
        // UnionType.AND is 0
        expect(generateOrFiltersMock).toHaveBeenCalledWith(0, mockFilters);
    });

    it('should call useGetSearchResultsForMultipleQuery with correct variables', () => {
        const searchQuery = 'query';
        renderHook(() => useGetAssetResults({ searchQuery, appliedFilters: mockAppliedFilters }));
        expect(useGetSearchResultsForMultipleQueryMock).toHaveBeenCalledWith({
            variables: {
                input: {
                    query: searchQuery,
                    start: 0,
                    count: 10,
                    orFilters: mockOrFilters,
                    searchFlags: {
                        skipCache: true,
                    },
                },
            },
        });
    });

    it('should use * as query if searchQuery is undefined', () => {
        renderHook(() => useGetAssetResults({ searchQuery: undefined, appliedFilters: mockAppliedFilters }));
        expect(useGetSearchResultsForMultipleQueryMock).toHaveBeenCalledWith(
            expect.objectContaining({
                variables: expect.objectContaining({
                    input: expect.objectContaining({
                        query: '*',
                    }),
                }),
            }),
        );
    });

    it('should return entities from data', () => {
        const { result } = renderHook(() =>
            useGetAssetResults({ searchQuery: 'query', appliedFilters: mockAppliedFilters }),
        );
        expect(result.current.entities).toEqual(mockData.searchAcrossEntities.searchResults.map((res) => res.entity));
    });

    it('should return empty array if data is undefined', () => {
        useGetSearchResultsForMultipleQueryMock.mockReturnValue({ data: undefined });
        const { result } = renderHook(() =>
            useGetAssetResults({ searchQuery: 'query', appliedFilters: mockAppliedFilters }),
        );
        expect(result.current.entities).toEqual([]);
    });
});
