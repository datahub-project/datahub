import { useHomeRecommendations } from '@app/homeV2/useHomeRecommendations';

import { Domain } from '@types';

const DOMAINS_MODULE_ID = 'Domains';
const MAX_DOMAINS = 5;

export const useGetDomains = (): {
    domains: { entity: Domain; assetCount: number }[];
    loading: boolean;
    refetch: () => Promise<unknown>;
} => {
    const { modules, loading, refetch } = useHomeRecommendations();

    const domainsModule = modules?.find((module) => module.moduleId === DOMAINS_MODULE_ID);
    const domains =
        domainsModule?.content
            ?.filter((content) => content.entity)
            .map((content) => ({
                entity: content.entity as Domain,
                assetCount: content.params?.contentParams?.count || 0,
            }))
            ?.slice(0, MAX_DOMAINS) || [];
    return { domains, loading, refetch };
};
