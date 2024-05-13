import { useListQueriesQuery } from '../../../../../../graphql/query.generated';
import { CorpUser, QuerySource, SortOrder } from '../../../../../../types.generated';
import { useIsSeparateSiblingsMode } from '../../../useIsSeparateSiblingsMode';
import { MAX_QUERIES_COUNT } from './utils/constants';
import { filterQueries } from './utils/filterQueries';

interface Props {
    entityUrn?: string;
    siblingUrn?: string;
    filterText: string;
}

export const usePopularQueries = ({ entityUrn, siblingUrn, filterText }: Props) => {
    const isSeparateSiblings = useIsSeparateSiblingsMode();

    const { data: popularQueriesData, loading } = useListQueriesQuery({
        variables: {
            input: {
                datasetUrn: entityUrn,
                start: 0,
                count: MAX_QUERIES_COUNT,
                source: QuerySource.System,
                sortInput: { sortCriterion: { field: 'runsPercentileLast30days', sortOrder: SortOrder.Descending } },
            },
        },
        skip: !entityUrn,
        fetchPolicy: 'cache-first',
    });

    const { data: siblingPopularQueriesData, loading: siblingsLoading } = useListQueriesQuery({
        variables: {
            input: {
                datasetUrn: siblingUrn,
                start: 0,
                count: MAX_QUERIES_COUNT,
                source: QuerySource.System,
                sortInput: { sortCriterion: { field: 'runsPercentileLast30days', sortOrder: SortOrder.Descending } },
            },
        },
        skip: !siblingUrn || isSeparateSiblings,
        fetchPolicy: 'cache-first',
    });

    const popularQueriesList = [
        ...(popularQueriesData?.listQueries?.queries || []),
        ...(siblingPopularQueriesData?.listQueries?.queries || []),
    ];

    const popularQueries = filterQueries(
        filterText,
        popularQueriesList
            .filter((q) => q.usageFeatures?.runsPercentileLast30days)
            .map((queryEntity) => ({
                urn: queryEntity.urn,
                title: queryEntity.properties?.name || undefined,
                description: queryEntity.properties?.description || undefined,
                query: queryEntity.properties?.statement?.value || '',
                createdTime: queryEntity?.properties?.created?.time,
                createdBy: queryEntity?.properties?.createdOn?.actor as CorpUser,
                usedBy: (queryEntity?.usageFeatures?.topUsersLast30Days || []) as CorpUser[],
                lastRun: queryEntity?.usageFeatures?.lastExecutedAt,
                runsPercentileLast30days: queryEntity?.usageFeatures?.runsPercentileLast30days,
            })),
    );

    return { popularQueries, loading: loading || siblingsLoading };
};
