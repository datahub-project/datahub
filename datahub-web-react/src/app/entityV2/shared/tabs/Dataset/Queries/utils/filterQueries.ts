import { FacetFilterInput } from '../../../../../../../types.generated';
import { Query } from '../types';

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
