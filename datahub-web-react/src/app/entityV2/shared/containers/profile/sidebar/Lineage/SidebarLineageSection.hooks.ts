import { useApolloClient } from '@apollo/client';
import { useContext, useMemo } from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import EntitySidebarContext, { SearchResultLineageCounts } from '@app/sharedV2/EntitySidebarContext';

import {
    GetSearchAcrossLineageCountsDocument,
    GetSearchAcrossLineageCountsQuery,
    useGetLineageCountsQuery,
    useGetSearchAcrossLineageCountsQuery,
} from '@graphql/lineage.generated';

type LineageCount = NonNullable<SearchResultLineageCounts['upstream']>;

const getVisibleCount = (count?: LineageCount | null): number => (count?.total || 0) - (count?.filtered || 0);

function hasLineageCounts(entity?: SearchResultLineageCounts | null): entity is SearchResultLineageCounts {
    return entity?.upstream != null || entity?.downstream != null;
}

function hasPositiveTypeFacets(results?: GetSearchAcrossLineageCountsQuery['upstreams'] | null): boolean {
    const typeFacet = results?.facets?.find((facet) => facet.field === '_entityType' || facet.field === 'entity');
    return (typeFacet?.aggregations ?? []).some((aggregation) => (aggregation.count || 0) > 0);
}

function hasUsableTypeBreakdown(data?: GetSearchAcrossLineageCountsQuery | null): boolean {
    return hasPositiveTypeFacets(data?.upstreams) || hasPositiveTypeFacets(data?.downstreams);
}

function isLineageForUrn(
    counts: SearchResultLineageCounts | null | undefined,
    urn: string,
): counts is SearchResultLineageCounts {
    return hasLineageCounts(counts) && (!counts.urn || counts.urn === urn);
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

    const hasTypeBreakdown = hasUsableTypeBreakdown(typeBreakdownData);

    let cachedCounts: SearchResultLineageCounts | undefined;
    if (isLineageForUrn(searchResultLineage, urn)) {
        cachedCounts = searchResultLineage;
    } else if (entityData?.urn === urn && (entityData?.upstream || entityData?.downstream)) {
        cachedCounts = { urn, upstream: entityData.upstream, downstream: entityData.downstream };
    }

    const { data: networkCounts, loading: networkLoading } = useGetLineageCountsQuery({
        variables: { urn, separateSiblings, startTimeMillis },
        fetchPolicy: 'cache-first',
        skip: !enabled || skip || hasTypeBreakdown || hasLineageCounts(cachedCounts),
    });

    const countsEntity = (networkCounts?.entity as SearchResultLineageCounts | null | undefined) ?? cachedCounts;
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
