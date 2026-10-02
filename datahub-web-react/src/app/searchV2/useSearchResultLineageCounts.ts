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

    const { data, loading } = useGetSearchResultLineageCountsQuery({
        variables: { urns: queryUrns },
        skip,
        fetchPolicy: 'cache-first',
        errorPolicy: 'all',
    });

    const countsByUrn = useMemo(() => lineageCountsByUrn(data?.entities as LineageCountEntity[] | undefined), [data]);

    return {
        countsByUrn,
        loading: !skip && loading && countsByUrn.size === 0,
    };
}
