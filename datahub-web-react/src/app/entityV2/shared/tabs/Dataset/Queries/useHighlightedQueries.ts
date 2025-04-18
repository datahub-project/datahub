import { useListQueriesQuery } from '../../../../../../graphql/query.generated';
import { QueryEntity, QuerySource } from '../../../../../../types.generated';
import usePagination from '../../../../../sharedV2/pagination/usePagination';
import useSorting from '../../../../../sharedV2/sorting/useSorting';
import { DEFAULT_PAGE_SIZE } from './utils/constants';
import { filterQueries, getQueryEntitiesFilter } from './utils/filterQueries';
import { mapQuery } from './utils/mapQuery';

interface Props {
    entityUrn?: string;
    siblingUrn?: string;
    filterText: string;
}

export const useHighlightedQueries = ({ entityUrn, siblingUrn, filterText }: Props) => {
    const pagination = usePagination(DEFAULT_PAGE_SIZE);
    const { start, count } = pagination;
    const sorting = useSorting();
    const { sortField, sortOrder } = sorting;

    const entityFilter = getQueryEntitiesFilter(entityUrn, siblingUrn);
    const {
        data: highlightedQueriesData,
        client,
        loading,
    } = useListQueriesQuery({
        variables: {
            input: {
                start,
                count,
                source: QuerySource.Manual,
                orFilters: [{ and: [entityFilter] }],
                sortInput: sortField && sortOrder ? { sortCriterion: { field: sortField, sortOrder } } : undefined,
            },
        },
        skip: !entityUrn,
        fetchPolicy: 'cache-first',
    });

    const queries = [...(highlightedQueriesData?.listQueries?.queries || [])] as QueryEntity[];

    const highlightedQueries = filterQueries(
        filterText,
        queries.map((queryEntity) => mapQuery({ queryEntity, entityUrn, siblingUrn })),
    );

    const total = highlightedQueriesData?.listQueries?.total || 0;

    return { highlightedQueries, client, loading, total, pagination, sorting };
};
