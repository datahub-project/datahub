import { ListIngestionSourcesDocument, ListIngestionSourcesQuery } from '@graphql/ingestion.generated';

/**
 * Add an entry to the ListIngestionSources cache.
 */
export const addToListIngestionSourcesCache = (client, newSource, queryInputs) => {
    // Read the data from our cache for this query.
    const currData: ListIngestionSourcesQuery | null = client.readQuery({
        query: ListIngestionSourcesDocument,
        variables: {
            input: queryInputs,
        },
    });

    // Add our new source into the existing list.
    const newSources = [newSource, ...(currData?.listIngestionSources?.ingestionSources || [])];

    // Write our data back to the cache.
    client.writeQuery({
        query: ListIngestionSourcesDocument,
        variables: {
            input: queryInputs,
        },
        data: {
            listIngestionSources: {
                start: 0,
                count: (currData?.listIngestionSources?.count || 0) + 1,
                total: (currData?.listIngestionSources?.total || 0) + 1,
                ingestionSources: newSources,
            },
        },
    });
};

/**
 * Update an entry in the ListIngestionSources cache.
 */
export const updateListIngestionSourcesCache = (client, updatedSource, queryInputs) => {
    // Read the data from our cache for this query
    const currData: ListIngestionSourcesQuery | null = client.readQuery({
        query: ListIngestionSourcesDocument,
        variables: {
            input: queryInputs,
        },
    });

    if (!currData?.listIngestionSources?.ingestionSources) return;

    // Update the given source in the existing list
    const newSources = currData.listIngestionSources.ingestionSources.map((source) =>
        source.urn === updatedSource.urn ? updatedSource : source,
    );

    // Write our data back to the cache
    client.writeQuery({
        query: ListIngestionSourcesDocument,
        variables: {
            input: queryInputs,
        },
        data: {
            listIngestionSources: {
                ...currData.listIngestionSources,
                ingestionSources: newSources,
            },
        },
    });
};

/**
 * Remove an entry from the ListIngestionSources cache.
 */
export const removeFromListIngestionSourcesCache = (client, urn, page, pageSize, query) => {
    // Read the data from our cache for this query.
    const currData: ListIngestionSourcesQuery | null = client.readQuery({
        query: ListIngestionSourcesDocument,
        variables: {
            input: {
                start: (page - 1) * pageSize,
                count: pageSize,
                query,
            },
        },
    });

    // Remove the source from the existing sources set.
    const newSources = [
        ...(currData?.listIngestionSources?.ingestionSources || []).filter((source) => source.urn !== urn),
    ];

    // Write our data back to the cache.
    client.writeQuery({
        query: ListIngestionSourcesDocument,
        variables: {
            input: {
                start: (page - 1) * pageSize,
                count: pageSize,
                query,
            },
        },
        data: {
            listIngestionSources: {
                start: currData?.listIngestionSources?.start || 0,
                count: (currData?.listIngestionSources?.count || 1) - 1,
                total: (currData?.listIngestionSources?.total || 1) - 1,
                ingestionSources: newSources,
            },
        },
    });
};
