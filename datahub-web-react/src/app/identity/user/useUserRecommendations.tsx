import { useMemo } from 'react';

import { useGetUserRecommendationsQuery } from '@graphql/user.generated';

const MAX_RECOMMENDED_USERS = 100;

export function useUserRecommendations() {
    // Fetch recommended users
    const { data: userRecommendationsData } = useGetUserRecommendationsQuery({
        variables: {
            input: {
                start: 0,
                count: MAX_RECOMMENDED_USERS,
            },
        },
        fetchPolicy: 'cache-first',
    });

    const recommendedUsers = useMemo(() => {
        return userRecommendationsData?.listUsers?.users || [];
    }, [userRecommendationsData?.listUsers?.users]);

    return {
        recommendedUsers,
    };
}
