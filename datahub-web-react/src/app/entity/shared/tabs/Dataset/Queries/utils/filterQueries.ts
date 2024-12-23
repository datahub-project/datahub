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
            query.title?.toLowerCase()?.includes(lowerFilterText) ||
            query.description?.toLowerCase()?.includes(lowerFilterText) ||
            query.query?.toLowerCase()?.includes(lowerFilterText)
        );
    });
};
