import { useState } from 'react';

import { DEFAULT_PAGE_SIZE } from '@app/entityV2/shared/tabs/Dataset/Queries/utils/constants';
import {
    filterQueries,
    getAndFilters,
    getQueryEntitiesFilter,
    getTimeFilters,
} from '@app/entityV2/shared/tabs/Dataset/Queries/utils/filterQueries';
import { mapQuery } from '@app/entityV2/shared/tabs/Dataset/Queries/utils/mapQuery';
import { useQueryParamValue } from '@app/entityV2/shared/useQueryParamValue';
import usePagination from '@app/sharedV2/pagination/usePagination';
import useSorting from '@app/sharedV2/sorting/useSorting';

import { useListQueriesQuery } from '@graphql/query.generated';
import { FacetFilterInput, QueryEntity, QuerySource, SortOrder } from '@types';

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
    const { createdAtFilter, lastModifiedAtFilter } = getTimeFilters(30);

    const andCreatedAtFilters = getAndFilters(selectedColumnsFilter, selectedUsersFilter, [
        entityFilter,
        createdAtFilter,
    ]);
    const andLastModifiedAtFilters = getAndFilters(selectedColumnsFilter, selectedUsersFilter, [
        entityFilter,
        lastModifiedAtFilter,
    ]);

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

                orFilters: [{ and: andCreatedAtFilters }, { and: andLastModifiedAtFilters }],
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
