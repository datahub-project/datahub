import { useState } from 'react';
import { useListQueriesQuery } from '../../../../../../graphql/query.generated';
import { FacetFilterInput, QueryEntity, QuerySource, SortOrder } from '../../../../../../types.generated';
import { filterQueries, getAndFilters, getQueryEntitiesFilter } from './utils/filterQueries';
import usePagination from '../../../../../sharedV2/pagination/usePagination';
import { DEFAULT_PAGE_SIZE } from './utils/constants';
import { mapQuery } from './utils/mapQuery';
import useSorting from '../../../../../sharedV2/sorting/useSorting';
import { useQueryParamValue } from '../../../useQueryParamValue';

interface Props {
    entityUrn?: string;
    siblingUrn?: string;
    filterText: string;
    defaultSelectedColumns?: string[];
    defaultSelectedUsers?: string[];
}

export const usePopularQueries = ({
    entityUrn,
    siblingUrn,
    filterText,
    defaultSelectedColumns,
    defaultSelectedUsers,
}: Props) => {
    const columnFromQueryParam = useQueryParamValue('column') as string | null;
    const siblingColumnFromQueryParam = useQueryParamValue('siblingColumn') as string | null;
    let columnsFromQueryParams = columnFromQueryParam ? [decodeURI(columnFromQueryParam)] : [];
    columnsFromQueryParams = siblingColumnFromQueryParam
        ? [...columnsFromQueryParams, decodeURI(siblingColumnFromQueryParam)]
        : columnsFromQueryParams;
    const defaultColumnsFilter = {
        field: 'entities',
        values: [...(columnsFromQueryParams.length ? columnsFromQueryParams : defaultSelectedColumns || [])],
    };
    const [selectedColumnsFilter, setSelectedColumnsFilter] = useState<FacetFilterInput>(defaultColumnsFilter);
    const defaultUsersFilter = { field: 'topUsersLast30DaysFeature', values: [...(defaultSelectedUsers || [])] };
    const [selectedUsersFilter, setSelectedUsersFilter] = useState<FacetFilterInput>(defaultUsersFilter);

    const pagination = usePagination(DEFAULT_PAGE_SIZE);
    const { start, count } = pagination;
    const sorting = useSorting();
    const { sortField, sortOrder } = sorting;

    const entityFilter = getQueryEntitiesFilter(entityUrn, siblingUrn);
    const andFilters = getAndFilters(selectedColumnsFilter, selectedUsersFilter, [entityFilter]);
    const { data: popularQueriesData, loading } = useListQueriesQuery({
        variables: {
            input: {
                start,
                count,
                source: QuerySource.System,
                sortInput:
                    sortField && sortOrder
                        ? { sortCriterion: { field: sortField, sortOrder } }
                        : { sortCriterion: { field: 'runsPercentileLast30days', sortOrder: SortOrder.Descending } },

                orFilters: [{ and: andFilters }],
            },
        },
        skip: !entityUrn,
        fetchPolicy: 'cache-first',
    });

    const popularQueriesList = [...(popularQueriesData?.listQueries?.queries || [])] as QueryEntity[];

    const total = popularQueriesData?.listQueries?.total || 0;

    const popularQueries = filterQueries(
        filterText,
        popularQueriesList.map((queryEntity) => mapQuery({ queryEntity, entityUrn, siblingUrn })),
    );

    return {
        popularQueries,
        loading,
        selectedColumnsFilter,
        setSelectedColumnsFilter,
        pagination,
        total,
        sorting,
        selectedUsersFilter,
        setSelectedUsersFilter,
    };
};
