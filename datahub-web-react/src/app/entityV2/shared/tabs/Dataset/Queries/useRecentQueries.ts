/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
}

export const useRecentQueries = ({ entityUrn, siblingUrn, filterText }: Props) => {
    // const appConfig = useAppConfig();
    const isSeparateSiblings = useIsSeparateSiblingsMode();

    const { data: recentQueriesData, loading } = useGetRecentQueriesQuery({
        variables: { urn: entityUrn as string },
        skip: !entityUrn,
        fetchPolicy: 'cache-first',
    });

    const { data: siblingRecentQueriesData, loading: siblingsLoading } = useGetRecentQueriesQuery({
        variables: { urn: siblingUrn as string },
        skip: !siblingUrn || isSeparateSiblings,
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
