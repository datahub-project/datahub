import { useContext } from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import {
    LineageDirectionSummary,
    getDirectDownstreamSummary,
    getDirectUpstreamSummary,
} from '@app/entityV2/shared/containers/profile/sidebar/Lineage/utils';
import EntitySidebarContext, { SearchResultLineageCounts } from '@app/sharedV2/EntitySidebarContext';

import {
    GetSearchAcrossLineageCountsQuery,
    useGetLineageCountsQuery,
    useGetSearchAcrossLineageCountsQuery,
} from '@graphql/lineage.generated';
import { SearchAcrossLineageResults } from '@types';

type LineageCount = NonNullable<SearchResultLineageCounts['upstream']>;
type CachedTypeResults = GetSearchAcrossLineageCountsQuery['upstreams'];

// The relationship query reports every edge, including ones the graph hides.
const getVisibleCount = (count?: LineageCount | null): number => (count?.total || 0) - (count?.filtered || 0);

function hasLineageCounts(entity?: SearchResultLineageCounts | null): entity is SearchResultLineageCounts {
    return entity?.upstream != null || entity?.downstream != null;
}

function isLineageForUrn(
    counts: SearchResultLineageCounts | null | undefined,
    urn: string,
): counts is SearchResultLineageCounts {
    return hasLineageCounts(counts) && (!counts.urn || counts.urn === urn);
}

/**
 * The cached breakdown is only used to name the neighbors, never to count them: it comes from a
 * different query with different filtering, and it may be left over from an older profile visit.
 * Naming a set we did not count is how "2 datasets" ends up next to a count of 3, so the summary
 * is dropped unless its total matches the count we are about to render.
 */
function getMatchingTypeSummary(
    results: CachedTypeResults | null | undefined,
    count: number,
    toSummary: (results: SearchAcrossLineageResults) => LineageDirectionSummary,
): LineageDirectionSummary | undefined {
    if (!results || count <= 0 || (results.total || 0) !== count) {
        return undefined;
    }
    const summary = toSummary(results as SearchAcrossLineageResults);
    return summary.types.length > 0 ? summary : undefined;
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
    const { searchResultLineage } = useContext(EntitySidebarContext);
    const active = enabled && !skip;

    let cachedCounts: SearchResultLineageCounts | undefined;
    if (active && isLineageForUrn(searchResultLineage, urn)) {
        cachedCounts = searchResultLineage;
    } else if (active && entityData?.urn === urn && (entityData.upstream || entityData.downstream)) {
        cachedCounts = { urn, upstream: entityData.upstream, downstream: entityData.downstream };
    }

    const { data: networkCounts, loading } = useGetLineageCountsQuery({
        variables: { urn, separateSiblings, startTimeMillis },
        fetchPolicy: 'cache-first',
        skip: !active || hasLineageCounts(cachedCounts),
    });

    const { data: cachedTypeData } = useGetSearchAcrossLineageCountsQuery({
        variables: { urn, startTimeMillis },
        fetchPolicy: 'cache-only',
        errorPolicy: 'ignore',
        skip: !active,
    });

    const counts = (networkCounts?.entity as SearchResultLineageCounts | null | undefined) ?? cachedCounts;
    const directUpstreamCount = getVisibleCount(counts?.upstream);
    const directDownstreamCount = getVisibleCount(counts?.downstream);

    return {
        directUpstreamCount,
        directDownstreamCount,
        upstreamTypeSummary: getMatchingTypeSummary(
            cachedTypeData?.upstreams,
            directUpstreamCount,
            getDirectUpstreamSummary,
        ),
        downstreamTypeSummary: getMatchingTypeSummary(
            cachedTypeData?.downstreams,
            directDownstreamCount,
            getDirectDownstreamSummary,
        ),
        loading: !hasLineageCounts(cachedCounts) && loading,
    };
}
