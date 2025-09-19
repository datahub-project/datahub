import { WatchQueryFetchPolicy } from '@apollo/client';

import { useUserContext } from '@app/context/useUserContext';

import { useListRecommendationsQuery } from '@graphql/recommendations.generated';
import { CorpUser, Domain, ScenarioType } from '@types';

const DOMAINS_MODULE_ID = 'Domains';
const MAX_DOMAINS = 5;

export const useGetDomains = (
    user?: CorpUser | null,
    fetchPolicy?: WatchQueryFetchPolicy,
): { domains: { entity: Domain; assetCount: number }[]; loading: boolean } => {
    const { localState } = useUserContext();
    const { selectedViewUrn } = localState;
    const { data, loading } = useListRecommendationsQuery({
        variables: {
            input: {
                userUrn: user?.urn as string,
                requestContext: {
                    scenario: ScenarioType.Home,
                },
                limit: 10,
                viewUrn: selectedViewUrn,
            },
        },
        fetchPolicy: fetchPolicy ?? 'cache-first',
        nextFetchPolicy: 'cache-first',
        skip: !user?.urn,
    });

    const domainsModule = data?.listRecommendations?.modules?.find((module) => module.moduleId === DOMAINS_MODULE_ID);
    const domains =
        domainsModule?.content
            ?.filter((content) => content.entity)
            .map((content) => ({
                entity: content.entity as Domain,
                assetCount: content.params?.contentParams?.count || 0,
            }))
            ?.slice(0, MAX_DOMAINS) || [];
    return { domains, loading };
};
