/**
 * Returns the queries for the current page
 */
export const getQueriesForPage = (queries: any, page: number, pageSize: number) => {
    const start = (page - 1) * pageSize;
    const end = start + pageSize;
    return queries.length >= end ? queries.slice(start, end) : queries.slice(start, queries.length);
};
