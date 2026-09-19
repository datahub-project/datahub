import { useApolloClient } from '@apollo/client';
import { useContext, useMemo } from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import EntitySidebarContext from '@app/sharedV2/EntitySidebarContext';

import {
    GetSearchAcrossLineageCountsDocument,
    GetSearchAcrossLineageCountsQuery,
    useGetLineageCountsQuery,
    useGetSearchAcrossLineageCountsQuery,
} from '@graphql/lineage.generated';

type LineageCount = {
    filtered?: number | null;
    total?: number | null;
};

export type LineageCountEntity = {
    upstream?: LineageCount | null;
    downstream?: LineageCount | null;
};

export const getVisibleCount = (count?: LineageCount | null): number => (count?.total || 0) - (count?.filtered || 0);

function hasLineageCounts(entity?: LineageCountEntity | null): boolean {
    return entity?.upstream != null || entity?.downstream != null;
}

function readCachedSearchAcrossLineageCounts(
    client: ReturnType<typeof useApolloClient>,
    urn: string,
    startTimeMillis?: number | null,
): GetSearchAcrossLineageCountsQuery | null {
    try {
        return client.readQuery<GetSearchAcrossLineageCountsQuery>({
            query: GetSearchAcrossLineageCountsDocument,
            variables: { urn, startTimeMillis },
        });
    } catch {
        return null;
    }
}

type SearchSummaryLineageArgs = {
    enabled: boolean;
    urn: string;
    entityData?: GenericEntityProperties | null;
    separateSiblings: boolean;
    startTimeMillis?: number | null;
    skip: boolean;
};

export function useSearchSummaryLineage({
    enabled,
    urn,
    entityData,
    separateSiblings,
    startTimeMillis,
    skip,
}: SearchSummaryLineageArgs) {
    const client = useApolloClient();
    const { searchResultLineage } = useContext(EntitySidebarContext);

    const { data: cachedTypeQueryData } = useGetSearchAcrossLineageCountsQuery({
        variables: { urn, startTimeMillis },
        fetchPolicy: 'cache-only',
        errorPolicy: 'ignore',
        skip: !enabled || skip,
    });

    const typeBreakdownData = useMemo(() => {
        if (!enabled || skip) {
            return null;
        }
        return cachedTypeQueryData ?? readCachedSearchAcrossLineageCounts(client, urn, startTimeMillis);
    }, [cachedTypeQueryData, client, enabled, skip, startTimeMillis, urn]);

    const hasTypeBreakdown = !!(typeBreakdownData?.upstreams || typeBreakdownData?.downstreams);

    let cachedCounts: LineageCountEntity | undefined;
    if (hasLineageCounts(searchResultLineage)) {
        cachedCounts = searchResultLineage;
    } else if (entityData?.upstream || entityData?.downstream) {
        cachedCounts = { upstream: entityData.upstream, downstream: entityData.downstream };
    }

    const { data: networkCounts, loading: networkLoading } = useGetLineageCountsQuery({
        variables: { urn, separateSiblings, startTimeMillis },
        fetchPolicy: 'cache-first',
        skip: !enabled || skip || hasTypeBreakdown || hasLineageCounts(cachedCounts),
    });

    const countsEntity = (networkCounts?.entity as LineageCountEntity | null | undefined) ?? cachedCounts;
    const directUpstreamCount = hasTypeBreakdown
        ? typeBreakdownData?.upstreams?.total || 0
        : getVisibleCount(countsEntity?.upstream);
    const directDownstreamCount = hasTypeBreakdown
        ? typeBreakdownData?.downstreams?.total || 0
        : getVisibleCount(countsEntity?.downstream);

    return {
        typeBreakdownData,
        hasTypeBreakdown,
        directUpstreamCount,
        directDownstreamCount,
        loading: !hasTypeBreakdown && !hasLineageCounts(cachedCounts) && networkLoading,
    };
}
