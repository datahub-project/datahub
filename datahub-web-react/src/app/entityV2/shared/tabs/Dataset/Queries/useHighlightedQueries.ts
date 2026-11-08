import { useState } from 'react';

import { DEFAULT_PAGE_SIZE } from '@app/entityV2/shared/tabs/Dataset/Queries/utils/constants';
import { filterQueries, getQueryEntitiesFilter } from '@app/entityV2/shared/tabs/Dataset/Queries/utils/filterQueries';
import { mapQuery } from '@app/entityV2/shared/tabs/Dataset/Queries/utils/mapQuery';
import {
    queriesEntityKey,
    selectQueriesListData,
} from '@app/entityV2/shared/tabs/Dataset/Queries/utils/selectQueriesListData';
import usePagination from '@app/sharedV2/pagination/usePagination';
import useSorting from '@app/sharedV2/sorting/useSorting';

import { useListQueriesQuery } from '@graphql/query.generated';
import { QueryEntity, QuerySource } from '@types';

interface Props {
    entityUrn?: string;
    siblingUrn?: string;
    filterText: string;
    /** Skips the query rather than waiting on a result the actor can't see. */
    canViewQueries: boolean;
}

export const useHighlightedQueries = ({ entityUrn, siblingUrn, filterText, canViewQueries }: Props) => {
    const pagination = usePagination(DEFAULT_PAGE_SIZE);
    const { start, count } = pagination;
    const sorting = useSorting();
    const { sortField, sortOrder } = sorting;

    const entityFilter = getQueryEntitiesFilter(entityUrn, siblingUrn);

    const {
        data: newData,
        previousData,
        error,
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
        skip: !entityUrn || !canViewQueries,
        fetchPolicy: 'cache-first',
    });

    // Keep the previous page while paging or sorting the same dataset. Drop it when the dataset
    // changes or the request fails, so this tab cannot show another dataset's queries.
    const entityKey = queriesEntityKey(entityUrn, siblingUrn);
    const [loadedEntityKey, setLoadedEntityKey] = useState<string | undefined>(undefined);
    if (newData && loadedEntityKey !== entityKey) {
        setLoadedEntityKey(entityKey);
    }
    const highlightedQueriesData = selectQueriesListData({
        data: newData,
        previousData,
        error,
        entityKey,
        loadedEntityKey,
    });

    const queries = [...(highlightedQueriesData?.listQueries?.queries || [])] as QueryEntity[];

    const highlightedQueries = filterQueries(
        filterText,
        queries.map((queryEntity) => mapQuery({ queryEntity, entityUrn, siblingUrn })),
    );

    const total = highlightedQueriesData?.listQueries?.total || 0;

    return { highlightedQueries, client, loading, total, pagination, sorting };
};
