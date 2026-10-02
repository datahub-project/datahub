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
    const queryUrns = uniqueUrns(urns);
    const skip = hideLineage || queryUrns.length === 0;

    const { data, loading, error } = useGetSearchResultLineageCountsQuery({
        variables: { urns: queryUrns },
        skip,
        fetchPolicy: 'cache-first',
    });

    const countsByUrn = useMemo(() => {
        // On query failure leave the map empty so the sidebar can fall back to getLineageCounts.
        if (!data) {
            return lineageCountsByUrn(undefined);
        }
        return lineageCountsByUrn(data.entities as LineageCountEntity[] | undefined, queryUrns);
    }, [data, queryUrns]);

    return {
        countsByUrn,
        // Keep loading true across page turns even when the previous batch still has cached counts.
        loading: !skip && loading,
        error,
    };
}
