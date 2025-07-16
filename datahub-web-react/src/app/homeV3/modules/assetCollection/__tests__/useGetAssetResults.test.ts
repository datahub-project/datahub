import { renderHook } from '@testing-library/react-hooks';
import { Mock, beforeEach, describe, expect, it, vi } from 'vitest';

import useGetAssetResults from '@app/homeV3/modules/assetCollection/useGetAssetResults';
import { convertFiltersMapToFilters } from '@app/searchV2/filtersV2/utils';
import { UnionType } from '@app/searchV2/utils/constants';
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
        useGetSearchResultsForMultipleQueryMock.mockReturnValue({ data: mockData, loading: false });
    });

    it('should call convertFiltersMapToFilters with appliedFilters', () => {
        renderHook(() => useGetAssetResults({ searchQuery: 'query', appliedFilters: mockAppliedFilters }));
        expect(convertFiltersMapToFiltersMock).toHaveBeenCalledWith(mockAppliedFilters);
    });

    it('should call generateOrFilters with UnionType.AND and filters', () => {
        renderHook(() => useGetAssetResults({ searchQuery: 'query', appliedFilters: mockAppliedFilters }));
        expect(generateOrFiltersMock).toHaveBeenCalledWith(UnionType.AND, mockFilters);
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
                    searchFlags: { skipCache: true },
                },
            },
        });
    });

    it('should use * as query if searchQuery is undefined', () => {
        renderHook(() => useGetAssetResults({ searchQuery: undefined, appliedFilters: mockAppliedFilters }));
        expect(useGetSearchResultsForMultipleQueryMock).toHaveBeenCalledWith(
            expect.objectContaining({
                variables: expect.objectContaining({ input: expect.objectContaining({ query: '*' }) }),
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
        useGetSearchResultsForMultipleQueryMock.mockReturnValue({ data: undefined, loading: false });
        const { result } = renderHook(() =>
            useGetAssetResults({ searchQuery: 'query', appliedFilters: mockAppliedFilters }),
        );
        expect(result.current.entities).toEqual([]);
    });

    it('should return empty array if searchAcrossEntities is missing', () => {
        useGetSearchResultsForMultipleQueryMock.mockReturnValue({ data: {}, loading: false });
        const { result } = renderHook(() =>
            useGetAssetResults({ searchQuery: 'foo', appliedFilters: mockAppliedFilters }),
        );
        expect(result.current.entities).toEqual([]);
    });

    it('should return empty array if searchResults is missing', () => {
        useGetSearchResultsForMultipleQueryMock.mockReturnValue({ data: { searchAcrossEntities: {} }, loading: false });
        const { result } = renderHook(() =>
            useGetAssetResults({ searchQuery: 'foo', appliedFilters: mockAppliedFilters }),
        );
        expect(result.current.entities).toEqual([]);
    });

    it('should use loading from useGetSearchResultsForMultipleQuery', () => {
        useGetSearchResultsForMultipleQueryMock.mockReturnValue({ data: mockData, loading: true });
        const { result } = renderHook(() =>
            useGetAssetResults({ searchQuery: 'bar', appliedFilters: mockAppliedFilters }),
        );
        expect(result.current.loading).toBe(true);
    });

    it('should handle empty appliedFilters', () => {
        convertFiltersMapToFiltersMock.mockReturnValue([]);
        renderHook(() => useGetAssetResults({ searchQuery: 'foo', appliedFilters: new Map() }));
        expect(generateOrFiltersMock).toHaveBeenCalledWith(UnionType.AND, []);
    });

    it('should handle appliedFilters with multiple fields', () => {
        const multiFilters = [
            { field: 'testField', value: 'testValue' },
            { field: 'anotherField', value: 'anotherValue' },
        ];
        convertFiltersMapToFiltersMock.mockReturnValue(multiFilters);
        renderHook(() => useGetAssetResults({ searchQuery: 'foo', appliedFilters: new Map([['k', { filters: [] }]]) }));
        expect(generateOrFiltersMock).toHaveBeenCalledWith(UnionType.AND, multiFilters);
    });

    it('should pass through searchFlags.skipCache', () => {
        renderHook(() => useGetAssetResults({ searchQuery: 'baz', appliedFilters: mockAppliedFilters }));
        const call = useGetSearchResultsForMultipleQueryMock.mock.calls[0][0];
        expect(call.variables.input.searchFlags.skipCache).toBe(true);
    });

    it('should memoize filter conversion results (filters object)', () => {
        // Set up for checking memoization via rerender
        const { rerender } = renderHook(
            ({ searchQuery, appliedFilters }) => useGetAssetResults({ searchQuery, appliedFilters }),
            {
                initialProps: { searchQuery: 'foo', appliedFilters: mockAppliedFilters },
            },
        );
        rerender({ searchQuery: 'foo', appliedFilters: mockAppliedFilters });
        // convertFiltersMapToFilters should not be called additional times (React useMemo prevents it)
        expect(convertFiltersMapToFiltersMock).toHaveBeenCalledTimes(1);
    });

    it('should handle when searchResults is not an array (null)', () => {
        useGetSearchResultsForMultipleQueryMock.mockReturnValue({
            data: { searchAcrossEntities: { searchResults: null } },
            loading: false,
        });
        const { result } = renderHook(() =>
            useGetAssetResults({ searchQuery: 'result', appliedFilters: mockAppliedFilters }),
        );
        expect(result.current.entities).toEqual([]);
    });

    it('should use empty array for entities if searchResults is empty', () => {
        useGetSearchResultsForMultipleQueryMock.mockReturnValue({
            data: { searchAcrossEntities: { searchResults: [] } },
            loading: false,
        });
        const { result } = renderHook(() =>
            useGetAssetResults({ searchQuery: 'result', appliedFilters: mockAppliedFilters }),
        );
        expect(result.current.entities).toEqual([]);
    });
});
