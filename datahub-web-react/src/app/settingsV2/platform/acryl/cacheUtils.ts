import {
    GetSearchResultsForMultipleDocument,
    GetSearchResultsForMultipleQuery,
} from '../../../../graphql/search.generated';

const addToCache = (existingInstances, newInstance) => {
    return [...existingInstances, newInstance];
};

const addOrUpdateCache = (existingInstances, newInstance, newUrn) => {
    const instanceToWrite = {
        entity: {
            urn: newUrn,
            type: newInstance.type,
            details: {
                name: newInstance.name,
                json: newInstance.json,
            },
            __typename: 'DataHubConnection',
        },
        matchedFields: [],
        insights: [],
        extraProperties: [],
    };

    const newInstances = [...existingInstances];
    let didUpdate = false;
    const updatedInstances = newInstances.map((instance) => {
        if (instance.entity.urn === newUrn) {
            didUpdate = true;
            return instanceToWrite;
        }
        return instance;
    });
    return didUpdate ? updatedInstances : addToCache(existingInstances, instanceToWrite);
};

export const updateInstancesList = (client, inputs, newInstance, newUrn, searchAcrossEntities) => {
    //  Read the data from our cache for this query.
    const currData: GetSearchResultsForMultipleQuery | null = client.readQuery({
        query: GetSearchResultsForMultipleDocument,
        variables: {
            input: inputs,
        },
    });

    if (currData === null) {
        // If there's no cached data, the first load has not occurred. Let it occur naturally.
        return;
    }

    const existingInstances = currData?.searchAcrossEntities?.searchResults || [];
    const newInstances = addOrUpdateCache(existingInstances, newInstance, newUrn);

    // Write our data back to the cache.
    client.writeQuery({
        query: GetSearchResultsForMultipleDocument,
        variables: {
            input: inputs,
        },
        data: {
            searchAcrossEntities: {
                ...searchAcrossEntities,
                total: newInstances.length,
                searchResults: newInstances,
            },
        },
    });
};

export const removeFromInstancesList = (client, inputs, deleteUrn, searchAcrossEntities) => {
    const currData: GetSearchResultsForMultipleQuery | null = client.readQuery({
        query: GetSearchResultsForMultipleDocument,
        variables: {
            input: inputs,
        },
    });

    if (currData === null) {
        return;
    }

    const existingInstances = currData?.searchAcrossEntities?.searchResults || [];

    const newInstances = [...existingInstances.filter((instance) => instance.entity.urn !== deleteUrn)];

    client.writeQuery({
        query: GetSearchResultsForMultipleDocument,
        variables: {
            input: inputs,
        },
        data: {
            searchAcrossEntities: {
                ...searchAcrossEntities,
                total: newInstances.length,
                searchResults: newInstances,
            },
        },
    });
};
