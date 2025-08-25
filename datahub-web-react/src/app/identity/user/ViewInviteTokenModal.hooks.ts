import React from 'react';

import { generateUserRecommendations } from '@app/identity/user/ViewInviteTokenModal.utils';

import { useGetUserRecommendationsQuery } from '@graphql/user.generated';
import { CorpUser } from '@types';

/**
 * Hook to fetch and process user recommendations for the invite modal
 */
export const useUserRecommendations = (modalOpen: boolean, maxRecommendations = 100) => {
    // Fetch user recommendations based on usage features
    const { data: userRecommendationsData, loading } = useGetUserRecommendationsQuery({
        skip: !modalOpen,
        variables: {
            input: {
                start: 0,
                count: maxRecommendations,
                query: '', // TODO: only users with usage data not logged in last 30 days
            },
        },
    });

    // Filter and sort users by usage patterns
    const recommendedUsers = React.useMemo(() => {
        const users = userRecommendationsData?.listUsers?.users || [];
        return generateUserRecommendations(users as CorpUser[], maxRecommendations);
    }, [userRecommendationsData, maxRecommendations]);

    return {
        recommendedUsers,
        loading,
    };
};
