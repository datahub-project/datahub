import { useMemo } from 'react';

import { useGetUserRecommendationsQuery } from '@graphql/user.generated';
import { UserUsageSortField } from '@types';

const MAX_RECOMMENDED_USERS = 100;

export function useUserRecommendations() {
    // Fetch recommended users
    const { data: userRecommendationsData } = useGetUserRecommendationsQuery({
        variables: {
            input: {
                limit: MAX_RECOMMENDED_USERS,
                sortBy: UserUsageSortField.UsageTotalPast_30Days,
                platformFilter: null,
            },
        },
        fetchPolicy: 'cache-first',
    });

    const recommendedUsers = useMemo(() => {
        return userRecommendationsData?.getUserRecommendations?.users || [];
    }, [userRecommendationsData]);

    return {
        recommendedUsers,
    };
}
