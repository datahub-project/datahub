import { useMemo } from 'react';

import { useGetUserRecommendationsQuery } from '@graphql/user.generated';
import { UserUsageSortField } from '@types';

const DEFAULT_LIMIT = 6;
const DEFAULT_SORT = UserUsageSortField.UsageTotalPast_30Days;

export interface UseUserRecommendationsOptions {
    limit?: number;
    sortBy?: UserUsageSortField;
    platformFilter?: string | null;
    skip?: boolean;
}

export function useUserRecommendations(options?: UseUserRecommendationsOptions) {
    const { limit = DEFAULT_LIMIT, sortBy = DEFAULT_SORT, platformFilter = null, skip = false } = options || {};

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
                sortBy,
                platformFilter,
            },
        },
        fetchPolicy: 'cache-and-network', // Load fresh data when modal opens
        skip, // Skip query when modal is closed
    });

    const recommendedUsers = useMemo(() => {
        return userRecommendationsData?.getUserRecommendations?.users || [];
    }, [userRecommendationsData]);

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
