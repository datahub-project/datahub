import { useEffect, useMemo, useState } from 'react';

import { getGlobalInvitedUsers, subscribeToInvitedUsers } from '@app/identity/user/inviteUsersGlobalState';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { CorpUser, EntityType, FacetFilterInput, FilterOperator, SearchSortInput, SortOrder } from '@types';

// Default limit for user recommendations
const DEFAULT_LIMIT = 20;
const DEFAULT_SORT: SearchSortInput = {
    sortCriterion: {
        field: 'userUsageTotalPast30DaysFeature',
        sortOrder: SortOrder.Descending,
    },
};

export interface UseUserRecommendationsOptions {
    limit?: number;
    start?: number;
    query?: string;
    sortInput?: SearchSortInput;
    selectedPlatforms?: string[]; // Platform URNs for filtering
    skip?: boolean;
}

export function useUserRecommendations(options?: UseUserRecommendationsOptions) {
    const {
        limit = DEFAULT_LIMIT,
        start = 0,
        query,
        sortInput = DEFAULT_SORT,
        selectedPlatforms = [],
        skip = false,
    } = options || {};

    // Build filters from selected platforms for searchAcrossEntities
    const orFilters = useMemo(() => {
        // Common filters that apply to all user recommendations
        const commonFilters: FacetFilterInput[] = [
            // Exclude users with invitation status (server-side filtering)
            {
                field: 'invitationStatus',
                values: [''],
                condition: FilterOperator.Exists,
                negated: true,
            },
            // Only include users with usage data (greater than 0)
            {
                field: 'userUsageTotalPast30DaysFeature',
                values: ['0'],
                condition: FilterOperator.GreaterThan,
            },
        ];

        // Create two OR branches for inactive users:
        // 1. Users where active field doesn't exist (legacy users)
        // 2. Users where active=false (new users)
        const inactiveUsersOrFilters = [
            {
                and: [
                    ...commonFilters,
                    {
                        field: 'active',
                        values: [''],
                        condition: FilterOperator.Exists,
                        negated: true,
                    },
                ],
            },
            {
                and: [
                    ...commonFilters,
                    {
                        field: 'active',
                        values: ['false'],
                        condition: FilterOperator.Equal,
                    },
                ],
            },
        ];

        // If no platform filters, just use the inactive users OR filters
        if (selectedPlatforms.length === 0) {
            return inactiveUsersOrFilters;
        }

        // Platform filters - if multiple platforms selected, use OR logic
        const platformFilters: FacetFilterInput[] = selectedPlatforms.map((platform) => {
            const platformField = `platformUsageTotal.${platform}`;
            return {
                field: platformField,
                values: ['0'],
                condition: FilterOperator.GreaterThan,
            };
        });

        // Combine inactive users filters with platform filters
        // We need to create a cross-product: (inactive_branch_1 OR inactive_branch_2) AND (platform_1 OR platform_2 OR ...)
        // This becomes: (inactive_1 AND platform_1) OR (inactive_1 AND platform_2) OR (inactive_2 AND platform_1) OR ...
        if (selectedPlatforms.length > 0) {
            const result: Array<{ and: FacetFilterInput[] }> = [];

            // For each inactive user filter branch
            inactiveUsersOrFilters.forEach((inactiveFilter) => {
                // For each platform filter
                platformFilters.forEach((platformFilter) => {
                    // Create a new OR branch combining this inactive filter with this platform filter
                    result.push({
                        and: [...(inactiveFilter.and || []), platformFilter],
                    });
                });
            });

            return result;
        }

        // No platform filters - return the inactive users OR filters as-is
        return inactiveUsersOrFilters;
    }, [selectedPlatforms]);

    // Use searchAcrossEntities
    const searchInput = {
        types: [EntityType.CorpUser],
        query: query || '*',
        start,
        count: limit,
        orFilters,
        sortInput,
    };

    const {
        data: searchData,
        loading,
        error,
        refetch,
    } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: searchInput,
        },
        fetchPolicy: 'network-only', // Always fetch from network to ensure fresh data
        notifyOnNetworkStatusChange: true, // Ensure loading state is updated on refetch
        skip, // Skip query when modal is closed
    });

    // Track changes to global invited users to trigger re-filtering
    const [invitedUsersVersion, setInvitedUsersVersion] = useState(0);

    useEffect(() => {
        // Subscribe to changes in global invited users
        const unsubscribe = subscribeToInvitedUsers(() => {
            // Force re-render by updating version
            setInvitedUsersVersion((v) => v + 1);
        });

        return unsubscribe;
    }, []);

    const { recommendedUsers, totalRecommendedUsers } = useMemo(() => {
        const searchResults = searchData?.searchAcrossEntities?.searchResults || [];
        const total = searchData?.searchAcrossEntities?.total || 0;

        // Filter out globally invited users
        const globalInvitedUsers = getGlobalInvitedUsers();
        const filteredUsers = searchResults
            .map((result) => result.entity as CorpUser)
            .filter((user) => {
                // Check if user or their email has been invited
                if (globalInvitedUsers.has(user.urn)) return false;

                const userEmail = user.info?.email || user.properties?.email || user.username;
                if (userEmail && globalInvitedUsers.has(userEmail)) return false;

                return true;
            });

        return {
            recommendedUsers: filteredUsers,
            totalRecommendedUsers: total,
        };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [searchData, invitedUsersVersion]);

    return {
        recommendedUsers,
        totalRecommendedUsers,
        loading,
        error,
        refetch,
    };
}
