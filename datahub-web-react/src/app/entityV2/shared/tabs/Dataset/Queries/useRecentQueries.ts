import { useGetRecentQueriesQuery } from '../../../../../../graphql/dataset.generated';
import { useAppConfig } from '../../../../../useAppConfig';
import { useIsSeparateSiblingsMode } from '../../../useIsSeparateSiblingsMode';
import { DEFAULT_MAX_RECENT_QUERIES } from './utils/constants';
import { filterQueries } from './utils/filterQueries';
import getTopNQueries from './utils/getTopNQueries';

interface Props {
    entityUrn?: string;
    siblingUrn?: string;
    filterText: string;
}

export const useRecentQueries = ({ entityUrn, siblingUrn, filterText }: Props) => {
    const appConfig = useAppConfig();
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

    const recentQueries = filterQueries(
        filterText,
        (
            getTopNQueries(
                appConfig?.config?.visualConfig?.queriesTab?.queriesTabResultSize || DEFAULT_MAX_RECENT_QUERIES,
                recentQueriesBuckets,
            ) || []
        ).map((recentQuery) => ({
            query: recentQuery.query,
            lastRun: recentQuery.dateMs,
        })),
    );

    return { recentQueries, loading: loading || siblingsLoading };
};
