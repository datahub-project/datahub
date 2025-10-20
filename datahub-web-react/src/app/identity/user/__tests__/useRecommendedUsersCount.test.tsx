import { ApolloError } from '@apollo/client';
import { renderHook } from '@testing-library/react-hooks';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import { useRecommendedUsersCount } from '@app/identity/user/useRecommendedUsersCount';

import {
    GetSearchResultsForMultipleQueryHookResult,
    useGetSearchResultsForMultipleQuery,
} from '@graphql/search.generated';

// Mock the GraphQL hook
vi.mock('@graphql/search.generated', () => ({
    useGetSearchResultsForMultipleQuery: vi.fn(),
}));

const mockUseGetSearchResultsForMultipleQuery = vi.mocked(useGetSearchResultsForMultipleQuery);

// Mock console.warn to avoid cluttering test output
const originalWarn = console.warn;
beforeAll(() => {
    console.warn = vi.fn();
});
afterAll(() => {
    console.warn = originalWarn;
});

describe('useRecommendedUsersCount', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should return 0 count initially when loading', () => {
        mockUseGetSearchResultsForMultipleQuery.mockReturnValue({
            data: undefined,
            loading: true,
            error: undefined,
        } as GetSearchResultsForMultipleQueryHookResult);

        const { result } = renderHook(() => useRecommendedUsersCount());

        expect(result.current.recommendedUsersCount).toBe(0);
        expect(result.current.loading).toBe(true);
        expect(result.current.error).toBeUndefined();
    });

    it('should return correct count when data is available', () => {
        mockUseGetSearchResultsForMultipleQuery.mockReturnValue({
            data: {
                searchAcrossEntities: {
                    total: 42,
                    searchResults: [],
                },
            },
            loading: false,
            error: undefined,
        } as unknown as GetSearchResultsForMultipleQueryHookResult);

        const { result } = renderHook(() => useRecommendedUsersCount());

        expect(result.current.recommendedUsersCount).toBe(42);
        expect(result.current.loading).toBe(false);
        expect(result.current.error).toBeUndefined();
    });

    it('should return 0 and log warning when query fails', () => {
        const mockError = new Error('GraphQL Error');
        mockUseGetSearchResultsForMultipleQuery.mockReturnValue({
            data: undefined,
            loading: false,
            error: mockError,
        } as GetSearchResultsForMultipleQueryHookResult);

        const { result } = renderHook(() => useRecommendedUsersCount());

        expect(result.current.recommendedUsersCount).toBe(0);
        expect(result.current.loading).toBe(false);
        expect(result.current.error).toBe(mockError);
    });

    it('should skip query when skip option is true', () => {
        mockUseGetSearchResultsForMultipleQuery.mockReturnValue({
            data: undefined,
            loading: false,
            error: undefined,
        } as GetSearchResultsForMultipleQueryHookResult);

        const { result } = renderHook(() => useRecommendedUsersCount({ skip: true }));

        expect(result.current.recommendedUsersCount).toBe(0);
        expect(result.current.loading).toBe(false);
        // Verify skip was passed to the query
        expect(mockUseGetSearchResultsForMultipleQuery).toHaveBeenCalledWith(
            expect.objectContaining({
                skip: true,
            }),
        );
    });

    it('should return 0 when searchAcrossEntities is null', () => {
        mockUseGetSearchResultsForMultipleQuery.mockReturnValue({
            data: {
                searchAcrossEntities: null,
            },
            loading: false,
            error: undefined,
        } as GetSearchResultsForMultipleQueryHookResult);

        const { result } = renderHook(() => useRecommendedUsersCount());

        expect(result.current.recommendedUsersCount).toBe(0);
    });

    it('should return 0 when total is undefined', () => {
        mockUseGetSearchResultsForMultipleQuery.mockReturnValue({
            data: {
                searchAcrossEntities: {
                    total: undefined,
                    searchResults: [],
                },
            },
            loading: false,
            error: undefined,
        } as unknown as GetSearchResultsForMultipleQueryHookResult);

        const { result } = renderHook(() => useRecommendedUsersCount());

        expect(result.current.recommendedUsersCount).toBe(0);
    });

    it('should pass correct query parameters', () => {
        mockUseGetSearchResultsForMultipleQuery.mockReturnValue({
            data: {
                searchAcrossEntities: {
                    total: 5,
                    searchResults: [],
                },
            },
            loading: false,
            error: undefined,
        } as unknown as GetSearchResultsForMultipleQueryHookResult);

        renderHook(() => useRecommendedUsersCount());

        expect(mockUseGetSearchResultsForMultipleQuery).toHaveBeenCalledWith(
            expect.objectContaining({
                variables: expect.objectContaining({
                    input: expect.objectContaining({
                        query: '*',
                        start: 0,
                        count: 1, // Minimal fetch for count only
                        sortInput: {
                            sortCriterion: {
                                field: 'userUsageTotalPast30DaysFeature',
                                sortOrder: 'DESCENDING',
                            },
                        },
                    }),
                }),
                fetchPolicy: 'cache-and-network',
                nextFetchPolicy: 'cache-first',
                pollInterval: 12 * 60 * 60 * 1000, // 12 hours
                skip: false,
            }),
        );
    });

    it('should have correct filter structure for recommended users', () => {
        mockUseGetSearchResultsForMultipleQuery.mockReturnValue({
            data: {
                searchAcrossEntities: {
                    total: 0,
                    searchResults: [],
                },
            },
            loading: false,
            error: undefined,
        } as unknown as GetSearchResultsForMultipleQueryHookResult);

        renderHook(() => useRecommendedUsersCount());

        const callArgs = mockUseGetSearchResultsForMultipleQuery.mock.calls[0][0];
        const { orFilters } = callArgs?.variables?.input || {};

        // Should have 2 OR branches
        expect(orFilters).toHaveLength(2);

        // Both branches should have common filters
        const commonFilters = [
            {
                field: 'invitationStatus',
                values: [''],
                condition: 'EXISTS',
                negated: true,
            },
            {
                field: 'userUsageTotalPast30DaysFeature',
                values: ['0'],
                condition: 'GREATER_THAN',
            },
        ];

        // First branch: active field doesn't exist
        expect(orFilters?.[0]?.and).toContainEqual(commonFilters[0]);
        expect(orFilters?.[0]?.and).toContainEqual(commonFilters[1]);
        expect(orFilters?.[0]?.and).toContainEqual({
            field: 'active',
            values: [''],
            condition: 'EXISTS',
            negated: true,
        });

        // Second branch: active = false
        expect(orFilters?.[1]?.and).toContainEqual(commonFilters[0]);
        expect(orFilters?.[1]?.and).toContainEqual(commonFilters[1]);
        expect(orFilters?.[1]?.and).toContainEqual({
            field: 'active',
            values: ['false'],
            condition: 'EQUAL',
        });
    });

    it('should handle onError callback correctly', () => {
        mockUseGetSearchResultsForMultipleQuery.mockReturnValue({
            data: undefined,
            loading: false,
            error: undefined,
        } as GetSearchResultsForMultipleQueryHookResult);

        renderHook(() => useRecommendedUsersCount());

        // Verify onError callback is passed
        const callArgs = mockUseGetSearchResultsForMultipleQuery.mock.calls[0][0];
        expect(callArgs?.onError).toBeDefined();
        expect(typeof callArgs?.onError).toBe('function');

        // Simulate error callback
        const mockError = new ApolloError({ errorMessage: 'Test error' });
        callArgs?.onError?.(mockError);

        // Should have logged warning
        expect(console.warn).toHaveBeenCalledWith('Failed to fetch recommended users count:', mockError);
    });
});
