import { useMemo } from 'react';

import { useUserContext } from '@app/context/useUserContext';
import {
    HOME_V2_RECOMMENDATION_MODULE_IDS,
    collectHomeRecommendationModuleIds,
} from '@app/homeV2/homeRecommendationModules';
import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import { useShowHomePageRedesign } from '@app/homeV3/context/hooks/useShowHomePageRedesign';

import { ListRecommendationsQuery, useListRecommendationsQuery } from '@graphql/recommendations.generated';
import { RecommendationModuleId, ScenarioType } from '@types';

type RecommendationModuleFromQuery = NonNullable<
    NonNullable<ListRecommendationsQuery['listRecommendations']>['modules']
>[number];

type UseHomeRecommendationsResult = {
    modules: RecommendationModuleFromQuery[] | undefined;
    loading: boolean;
    refetch: () => Promise<unknown>;
};

/**
 * Single source of truth for home-page recommendations (Domains / Platforms / Home V2 terms).
 *
 * Search-bar recently viewed uses its own HOME query — do not consume this hook from chrome.
 */
export const useHomeRecommendations = (): UseHomeRecommendationsResult => {
    const { user, localState } = useUserContext();
    const { selectedViewUrn } = localState;
    const userUrn = user?.urn;
    const isHomeV3 = useShowHomePageRedesign();
    const { template } = usePageTemplateContext();

    const modulesFilter: RecommendationModuleId[] = useMemo(() => {
        if (!isHomeV3) {
            return HOME_V2_RECOMMENDATION_MODULE_IDS;
        }
        return collectHomeRecommendationModuleIds(template);
    }, [isHomeV3, template]);

    const { data, loading, refetch } = useListRecommendationsQuery({
        variables: {
            input: {
                userUrn: userUrn as string,
                requestContext: {
                    scenario: ScenarioType.Home,
                    modules: modulesFilter,
                },
                limit: modulesFilter.length,
                viewUrn: selectedViewUrn,
            },
        },
        fetchPolicy: 'cache-first',
        skip: !userUrn || modulesFilter.length === 0,
    });

    return {
        modules: data?.listRecommendations?.modules,
        loading,
        refetch,
    };
};
