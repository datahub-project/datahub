import { ListQueriesDocument, ListQueriesQuery } from '@graphql/query.generated';
import { QueryEntity } from '@types';

export const removeQueryFromListQueriesCache = (urn, client, page, pageSize, datasetUrn) => {
    const currData: ListQueriesQuery | null = client.readQuery({
        query: ListQueriesDocument,
        variables: {
            input: {
                start: (page - 1) * pageSize,
                count: pageSize,
                datasetUrn,
            },
        },
    });

    const newQueries = [...(currData?.listQueries?.queries || []).filter((query) => query.urn !== urn)];

    client.writeQuery({
        query: ListQueriesDocument,
        variables: {
            input: {
                start: (page - 1) * pageSize,
                count: pageSize,
                datasetUrn,
            },
        },
        data: {
            listQueries: {
                start: currData?.listQueries?.start || 0,
                count: (currData?.listQueries?.count || 1) - 1,
                total: (currData?.listQueries?.total || 1) - 1,
                queries: newQueries,
            },
        },
    });
};

export const updateListQueriesCache = (urn: string, newQuery: QueryEntity, client, page, pageSize, datasetUrn) => {
    // Read the data from our cache for this query.
    const currData: ListQueriesQuery | null = client.readQuery({
        query: ListQueriesDocument,
        variables: {
            input: {
                start: (page - 1) * pageSize,
                count: pageSize,
                datasetUrn,
            },
        },
    });

    if (currData === null) {
        // If there's no cached data, the first load has not occurred. Let it occur naturally.
        return;
    }

    const existingQueries = currData?.listQueries?.queries || [];
    const newQueries = [...existingQueries];
    const updatedQueries = newQueries.map((query) => {
        if (urn === query.urn) {
            return newQuery;
        }
        return query;
    });

    const currCount = currData?.listQueries?.count || 0;
    const currTotal = currData?.listQueries?.total || 0;
    const didAdd = newQueries.length > existingQueries.length;

    client.writeQuery({
        query: ListQueriesDocument,
        variables: {
            input: {
                start: 0,
                count: pageSize,
                datasetUrn,
            },
        },
        data: {
            listQueries: {
                start: currData?.listQueries?.start || 0,
                count: currCount + (didAdd ? 1 : 0),
                total: currTotal + (didAdd ? 1 : 0),
                queries: updatedQueries,
            },
        },
    });
};

export const addQueryToListQueriesCache = (query, client, pageSize, datasetUrn) => {
    const currData: ListQueriesQuery | null = client.readQuery({
        query: ListQueriesDocument,
        variables: {
            input: {
                start: 0,
                count: pageSize,
                datasetUrn,
            },
        },
    });

    const newQueries = [query, ...(currData?.listQueries?.queries || [])];

    client.writeQuery({
        query: ListQueriesDocument,
        variables: {
            input: {
                start: 0,
                count: pageSize,
                datasetUrn,
            },
        },
        data: {
            listQueries: {
                start: currData?.listQueries?.start || 0,
                count: (currData?.listQueries?.count || 1) + 1,
                total: (currData?.listQueries?.total || 1) + 1,
                queries: newQueries,
            },
        },
    });
};
