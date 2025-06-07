import { Query } from '@app/entityV2/shared/tabs/Dataset/Queries/types';
import { CREATED_TIME_FIELD, LAST_MODIFIED_TIME_FIELD } from '@app/searchV2/context/constants';

import { FacetFilterInput, FilterOperator } from '@types';

/**
 * Filter queries by a search string. Compares name, description, and query statement.
 *
 * @param filterText the search text
 * @param queries the queries to filter
 */
export const filterQueries = (filterText, queries: Query[]) => {
    const lowerFilterText = filterText.toLowerCase();
    return queries.filter((query) => {
        return (
            query.title?.toLowerCase().includes(lowerFilterText) ||
            query.description?.toLowerCase().includes(lowerFilterText) ||
            query.query?.toLowerCase()?.includes(lowerFilterText)
        );
    });
};

export const getQueryEntitiesFilter = (entityUrn?: string, siblingUrn?: string) => {
    const values = siblingUrn ? [entityUrn as string, siblingUrn] : [entityUrn as string];
    return { field: 'entities', values };
};

export const getAndFilters = (
    selectedColumnsFilter: FacetFilterInput,
    selectedUsersFilter: FacetFilterInput,
    existingFilters: FacetFilterInput[] = [],
) => {
    let andFilters = selectedColumnsFilter.values?.length
        ? [...existingFilters, selectedColumnsFilter]
        : existingFilters;
    andFilters = selectedUsersFilter.values?.length ? [...andFilters, selectedUsersFilter] : andFilters;
    return andFilters;
};

export const getTimeFilters = (daysAgo = 30) => {
    // Calculate epoch time in ms from X days ago
    const today = new Date();
    today.setHours(0, 0, 0, 0); // Set to start of today for caching
    const timestamp = today.getTime() - daysAgo * 24 * 60 * 60 * 1000;

    const createdAtFilter = {
        field: CREATED_TIME_FIELD,
        condition: FilterOperator.GreaterThanOrEqualTo,
        values: [timestamp.toString()],
    };

    const lastModifiedAtFilter = {
        field: LAST_MODIFIED_TIME_FIELD,
        condition: FilterOperator.GreaterThanOrEqualTo,
        values: [timestamp.toString()],
    };

    return { createdAtFilter, lastModifiedAtFilter };
};
