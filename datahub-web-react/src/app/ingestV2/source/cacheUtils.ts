import deepmerge from 'deepmerge';

import { ListIngestionSourcesDocument, ListIngestionSourcesQuery } from '@graphql/ingestion.generated';

/**
 * Deeply merges or adds a source to the list by urn
 */
export const mergeSources = (updatedSource, existingSources, shouldReplace = false) => {
    let found = false;
    const merged = existingSources.map((source) => {
        if (source.urn === updatedSource.urn) {
            found = true;
            return shouldReplace ? updatedSource : deepmerge(source, updatedSource);
        }
        return source;
    });
    return found ? merged : [updatedSource, ...merged];
};

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
    const newSources = mergeSources(newSource, currData?.listIngestionSources?.ingestionSources, true);

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
export const updateListIngestionSourcesCache = (client, updatedSource, queryInputs, shouldReplace = false) => {
    // Read the data from our cache for this query
    const currData: ListIngestionSourcesQuery | null = client.readQuery({
        query: ListIngestionSourcesDocument,
        variables: {
            input: queryInputs,
        },
    });

    if (!currData?.listIngestionSources?.ingestionSources) return;

    const newSources = mergeSources(updatedSource, currData.listIngestionSources.ingestionSources, shouldReplace);

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
