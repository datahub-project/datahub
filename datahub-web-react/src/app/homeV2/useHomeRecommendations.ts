import { useUserContext } from '@app/context/useUserContext';

import { ListRecommendationsQuery, useListRecommendationsQuery } from '@graphql/recommendations.generated';
import { ScenarioType } from '@types';

type RecommendationModuleFromQuery = NonNullable<
    NonNullable<ListRecommendationsQuery['listRecommendations']>['modules']
>[number];

type UseHomeRecommendationsResult = {
    modules: RecommendationModuleFromQuery[] | undefined;
    loading: boolean;
    refetch: () => void;
};

/**
 * Single source of truth for home-page recommendations.
 *
 * Fires one `listRecommendations` request per unique (userUrn, viewUrn) pair.
 * All home-page hooks should consume this hook so Apollo can deduplicate
 * the network request across the component tree.
 */
export const useHomeRecommendations = (): UseHomeRecommendationsResult => {
    const { user, localState } = useUserContext();
    const { selectedViewUrn } = localState;
    const userUrn = user?.urn;

    const { data, loading, refetch } = useListRecommendationsQuery({
        variables: {
            input: {
                userUrn: userUrn as string,
                requestContext: {
                    scenario: ScenarioType.Home,
                },
                limit: 10,
                viewUrn: selectedViewUrn,
            },
        },
        fetchPolicy: 'cache-first',
        skip: !userUrn,
    });

    return {
        modules: data?.listRecommendations?.modules,
        loading,
        refetch,
    };
};
