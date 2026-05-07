import { WatchQueryFetchPolicy } from '@apollo/client';
import { useEffect } from 'react';

import { useHomeRecommendations } from '@app/homeV2/useHomeRecommendations';

import { CorpUser, Domain } from '@types';

const DOMAINS_MODULE_ID = 'Domains';
const MAX_DOMAINS = 5;

export const useGetDomains = (
    _user?: CorpUser | null,
    fetchPolicy?: WatchQueryFetchPolicy,
): { domains: { entity: Domain; assetCount: number }[]; loading: boolean } => {
    const { modules, loading, refetch } = useHomeRecommendations();

    // homeV3 passes 'cache-and-network' when a module reload is triggered.
    // Since useHomeRecommendations uses cache-first, an explicit refetch achieves
    // the same effect: return cached data immediately, then update from the network.
    useEffect(() => {
        if (fetchPolicy === 'cache-and-network') {
            refetch();
        }
    }, [fetchPolicy, refetch]);

    const domainsModule = modules?.find((module) => module.moduleId === DOMAINS_MODULE_ID);
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
