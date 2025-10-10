import { useEffect, useMemo, useState } from 'react';

import { buildRecommendedUsersFilters } from '@app/identity/user/buildRecommendedUsersFilters';
import { getGlobalInvitedUsers, subscribeToInvitedUsers } from '@app/identity/user/inviteUsersGlobalState';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { CorpUser, EntityType, SearchSortInput, SortOrder } from '@types';

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

    const orFilters = useMemo(() => buildRecommendedUsersFilters({ selectedPlatforms }), [selectedPlatforms]);

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
