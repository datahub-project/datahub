import { useMemo } from 'react';

import { buildRecommendedUsersFilters } from '@app/identity/user/buildRecommendedUsersFilters';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { EntityType, SearchSortInput, SortOrder } from '@types';

const TWELVE_HOURS_MS = 12 * 60 * 60 * 1000;

const DEFAULT_SORT: SearchSortInput = {
    sortCriterion: {
        field: 'userUsageTotalPast30DaysFeature',
        sortOrder: SortOrder.Descending,
    },
};

export interface UseRecommendedUsersCountOptions {
    skip?: boolean; // Skip the query entirely
}

/**
 * Lightweight hook to get only the count of recommended users.
 * Uses minimal data fetching (count: 1) and aggressive caching.
 * Handles errors gracefully to prevent crashes.
 */
export function useRecommendedUsersCount(options?: UseRecommendedUsersCountOptions) {
    const { skip = false } = options || {};

    const orFilters = useMemo(() => buildRecommendedUsersFilters(), []);

    const searchInput = {
        types: [EntityType.CorpUser],
        query: '*',
        start: 0,
        count: 1, // Minimal - we only need the total
        orFilters,
        sortInput: DEFAULT_SORT,
    };

    const {
        data: searchData,
        loading,
        error,
    } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: searchInput,
        },
        fetchPolicy: 'cache-and-network', // Show cached data immediately, update in background
        nextFetchPolicy: 'cache-first', // Subsequent fetches use cache first
        pollInterval: TWELVE_HOURS_MS, // Refresh every 12 hours
        skip, // Don't run query if skip is true
        // Silently handle errors - don't crash the app
        onError: (err) => {
            console.warn('Failed to fetch recommended users count:', err);
        },
    });

    const count = useMemo(() => {
        // If there's an error or no data, return 0 (don't show badge)
        if (error || !searchData?.searchAcrossEntities) {
            return 0;
        }
        return searchData.searchAcrossEntities.total || 0;
    }, [searchData, error]);

    return {
        recommendedUsersCount: count,
        loading,
        error,
    };
}
