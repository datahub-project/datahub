import { useMemo } from 'react';

import { useGetUserRecommendationsQuery } from '@graphql/user.generated';
import { UserUsageSortField } from '@types';

const DEFAULT_LIMIT = 20;
const DEFAULT_SORT = UserUsageSortField.UsageTotalPast_30Days;

export interface UseUserRecommendationsOptions {
    limit?: number;
    start?: number;
    query?: string;
    sortBy?: UserUsageSortField;
    platformFilter?: string | null;
    skip?: boolean;
    excludeInvitedUsers?: Set<string>; // Server-side filtering for invited users (gamification)
}

export function useUserRecommendations(options?: UseUserRecommendationsOptions) {
    const {
        limit = DEFAULT_LIMIT,
        start = 0,
        query,
        sortBy = DEFAULT_SORT,
        platformFilter = null,
        skip = false,
        excludeInvitedUsers = new Set(),
    } = options || {};

    // Fetch recommended users
    const {
        data: userRecommendationsData,
        loading,
        error,
        refetch,
    } = useGetUserRecommendationsQuery({
        variables: {
            input: {
                limit,
                start,
                query,
                sortBy,
                platformFilter,
            },
        },
        fetchPolicy: 'cache-and-network', // Load fresh data when parameters change
        notifyOnNetworkStatusChange: true, // Ensure loading state is updated on refetch
        skip, // Skip query when modal is closed
    });

    const recommendedUsers = useMemo(() => {
        const allUsers = userRecommendationsData?.getUserRecommendations?.users || [];

        // Filter out invited users - let count decrease naturally for gamification
        return allUsers.filter((user) => {
            if (excludeInvitedUsers.has(user.urn)) return false;
            if (user.username && excludeInvitedUsers.has(user.username)) return false;
            if (user.info?.email && excludeInvitedUsers.has(user.info.email)) return false;
            if (user.properties?.email && excludeInvitedUsers.has(user.properties.email)) return false;
            return true;
        });
    }, [userRecommendationsData, excludeInvitedUsers]);

    const totalRecommendedUsers = useMemo(() => {
        return userRecommendationsData?.getUserRecommendations?.total || 0;
    }, [userRecommendationsData]);

    return {
        recommendedUsers,
        totalRecommendedUsers,
        loading,
        error,
        refetch,
    };
}
