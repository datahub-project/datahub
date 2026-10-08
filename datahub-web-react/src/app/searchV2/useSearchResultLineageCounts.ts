import { useMemo } from 'react';

import { LineageCountEntity, lineageCountsByUrn } from '@app/searchV2/SearchResults.utils';
import { useHideLineageInSearchCards } from '@app/useAppConfig';

import { useGetSearchResultLineageCountsQuery } from '@graphql/lineage.generated';

function uniqueUrns(urns: string[]): string[] {
    return Array.from(new Set(urns.filter((urn) => urn.length > 0)));
}

/**
 * Lineage totals for the results already rendered. Skipped entirely when cards hide lineage.
 * While this is in flight the search sidebar should wait instead of issuing its own count.
 */
export function useSearchResultLineageCounts(urns: string[]) {
    const hideLineage = useHideLineageInSearchCards();
    // Stabilize identity so the counts memo is not invalidated every parent render.
    const queryUrnsKey = uniqueUrns(urns).join('\0');
    const queryUrns = useMemo(() => (queryUrnsKey.length > 0 ? queryUrnsKey.split('\0') : []), [queryUrnsKey]);
    const skip = hideLineage || queryUrns.length === 0;

    const { data, loading, error } = useGetSearchResultLineageCountsQuery({
        variables: { urns: queryUrns },
        skip,
        fetchPolicy: 'cache-first',
    });

    const countsByUrn = useMemo(() => {
        // On query failure leave the map empty so the sidebar can fall back to getLineageCounts
        // and search cards can drop the reserved badge slot.
        if (error || !data) {
            return lineageCountsByUrn(undefined);
        }
        const entities = data.entities as LineageCountEntity[] | undefined;
        // While loading, Apollo may still expose the prior page's `data`. Index complete
        // pairs from that payload, but do not settle against the current URN set — that
        // would write zeros for new URNs and flash "no lineage" badges until the refetch lands.
        if (loading) {
            return lineageCountsByUrn(entities);
        }
        return lineageCountsByUrn(entities, queryUrns);
    }, [data, queryUrns, loading, error]);

    return {
        countsByUrn,
        // Keep loading true across page turns even when the previous batch still has cached counts.
        loading: !skip && loading,
        error,
    };
}
