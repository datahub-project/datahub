import React from 'react';

import { generateUserRecommendations } from '@app/identity/user/ViewInviteTokenModal.utils';

import { useGetUserRecommendationsQuery } from '@graphql/user.generated';
import { CorpUser } from '@types';

const MAX_RECOMMENDATIONS = 500;
/**
 * Hook to fetch and process user recommendations for the invite modal
 */
export const useUserRecommendations = (modalOpen: boolean, maxRecommendations = MAX_RECOMMENDATIONS) => {
    // Fetch user recommendations based on usage features
    const {
        data: userRecommendationsData,
        loading,
        error,
    } = useGetUserRecommendationsQuery({
        skip: !modalOpen,
        variables: {
            input: {
                start: 0,
                count: MAX_RECOMMENDATIONS,
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
        error,
    };
};
