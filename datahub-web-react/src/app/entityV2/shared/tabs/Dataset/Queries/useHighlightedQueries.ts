import { useListQueriesQuery } from '../../../../../../graphql/query.generated';
import { CorpUser, QuerySource } from '../../../../../../types.generated';
import { useIsSeparateSiblingsMode } from '../../../useIsSeparateSiblingsMode';
import { MAX_QUERIES_COUNT } from './utils/constants';
import { filterQueries } from './utils/filterQueries';

interface Props {
    entityUrn?: string;
    siblingUrn?: string;
    filterText: string;
}

export const useHighlightedQueries = ({ entityUrn, siblingUrn, filterText }: Props) => {
    const isSeparateSiblings = useIsSeparateSiblingsMode();

    const {
        data: highlightedQueriesData,
        client,
        loading,
    } = useListQueriesQuery({
        variables: {
            input: {
                datasetUrn: entityUrn,
                start: 0,
                count: MAX_QUERIES_COUNT,
                source: QuerySource.Manual,
            },
        },
        skip: !entityUrn,
        fetchPolicy: 'cache-first',
    });

    const { data: siblingHighlightedQueriesData, loading: siblingsLoading } = useListQueriesQuery({
        variables: {
            input: { datasetUrn: siblingUrn, start: 0, count: MAX_QUERIES_COUNT, source: QuerySource.Manual },
        },
        skip: !siblingUrn || isSeparateSiblings,
        fetchPolicy: 'cache-first',
    });

    const queries = [
        ...(highlightedQueriesData?.listQueries?.queries || []),
        ...(siblingHighlightedQueriesData?.listQueries?.queries || []),
    ];

    const highlightedQueries = filterQueries(
        filterText,
        queries.map((queryEntity) => ({
            urn: queryEntity.urn,
            title: queryEntity.properties?.name || undefined,
            description: queryEntity.properties?.description || undefined,
            query: queryEntity.properties?.statement?.value || '',
            createdTime: queryEntity?.properties?.created?.time,
            createdBy: queryEntity?.properties?.createdOn?.actor as CorpUser,
            lastRun: queryEntity?.usageFeatures?.lastExecutedAt,
        })),
    );

    return { highlightedQueries, client, loading: loading || siblingsLoading };
};
