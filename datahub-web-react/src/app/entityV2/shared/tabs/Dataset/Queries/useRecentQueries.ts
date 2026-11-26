import { MAX_QUERIES_COUNT } from '@app/entityV2/shared/tabs/Dataset/Queries/utils/constants';
import { filterQueries } from '@app/entityV2/shared/tabs/Dataset/Queries/utils/filterQueries';
import getTopNQueries from '@app/entityV2/shared/tabs/Dataset/Queries/utils/getTopNQueries';
// import { useAppConfig } from '../../../../../useAppConfig';
import { useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';

import { useGetRecentQueriesQuery } from '@graphql/dataset.generated';

interface Props {
    entityUrn?: string;
    siblingUrn?: string;
    filterText: string;
    /**
     * This query selects usageStats.topSqlQueries, which the server denies with a GraphQL
     * AuthorizationException (not a graceful empty result) when the actor lacks
     * VIEW_ENTITY_QUERIES/VIEW_ALL_QUERIES on the dataset.
     */
    canViewQueries: boolean;
}

export const useRecentQueries = ({ entityUrn, siblingUrn, filterText, canViewQueries }: Props) => {
    // const appConfig = useAppConfig();
    const isSeparateSiblings = useIsSeparateSiblingsMode();

    const { data: recentQueriesData, loading } = useGetRecentQueriesQuery({
        variables: { urn: entityUrn as string },
        skip: !entityUrn || !canViewQueries,
        fetchPolicy: 'cache-first',
    });

    const { data: siblingRecentQueriesData, loading: siblingsLoading } = useGetRecentQueriesQuery({
        variables: { urn: siblingUrn as string },
        skip: !siblingUrn || isSeparateSiblings || !canViewQueries,
        fetchPolicy: 'cache-first',
    });

    const recentQueriesBuckets = [
        ...(recentQueriesData?.dataset?.usageStats?.buckets || []),
        ...(siblingRecentQueriesData?.dataset?.usageStats?.buckets || []),
    ];

    recentQueriesBuckets.sort((bucketA, bucketB) => (bucketB?.bucket || 0) - (bucketA?.bucket || 0));

    const recentQueries = filterQueries(
        filterText,
        (
            getTopNQueries(
                // TODO: uncomment later
                // appConfig?.config?.visualConfig?.queriesTab?.queriesTabResultSize || DEFAULT_MAX_RECENT_QUERIES,
                MAX_QUERIES_COUNT,
                recentQueriesBuckets,
            ) || []
        ).map((recentQuery) => ({
            query: recentQuery.query,
            lastRun: recentQuery.dateMs,
        })),
    );

    return { recentQueries, loading: loading || siblingsLoading };
};
