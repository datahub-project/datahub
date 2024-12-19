import { GetSearchResultsForMultipleDocument, GetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';

const addToCache = (existingProperties, newProperty) => {
    const propertyToWrite = {
        entity: {
            urn: newProperty.urn,
            type: newProperty.type,
            definition: {
                displayName: newProperty.definition.displayName,
                qualifiedName: newProperty.definition.qualifiedName,
                description: newProperty.definition.description,
                cardinality: newProperty.definition.cardinality,
                immutable: newProperty.definition.immutable,
                valueType: newProperty.definition.valueType,
                entityTypes: newProperty.definition.entityTypes,
                typeQualifier: newProperty.definition.typeQualifier,
                allowedValues: newProperty.definition.allowedValues,
                created: newProperty.definition.created,
                lastModified: newProperty.definition.lastModified,
            },
            settings: {
                isHidden: newProperty.settings.isHidden,
                showInSearchFilters: newProperty.settings.showInSearchFilters,
                showAsAssetBadge: newProperty.settings.showAsAssetBadge,
                showInAssetSummary: newProperty.settings.showInAssetSummary,
                showInColumnsTable: newProperty.settings.showInColumnsTable,
            },
            __typename: 'StructuredPropertyEntity',
        },
        matchedFields: [],
        insights: [],
        extraProperties: [],
        __typename: 'SearchResult',
    };

    return [propertyToWrite, ...existingProperties];
};

export const updatePropertiesList = (client, inputs, newProperty, searchAcrossEntities) => {
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

    const existingProperties = currData?.searchAcrossEntities?.searchResults || [];
    const newProperties = addToCache(existingProperties, newProperty);

    // Write our data back to the cache.
    client.writeQuery({
        query: GetSearchResultsForMultipleDocument,
        variables: {
            input: inputs,
        },
        data: {
            searchAcrossEntities: {
                ...searchAcrossEntities,
                total: newProperties.length,
                searchResults: newProperties,
            },
        },
    });
};

export const removeFromPropertiesList = (client, inputs, deleteUrn, searchAcrossEntities) => {
    const currData: GetSearchResultsForMultipleQuery | null = client.readQuery({
        query: GetSearchResultsForMultipleDocument,
        variables: {
            input: inputs,
        },
    });

    if (currData === null) {
        return;
    }

    const existingProperties = currData?.searchAcrossEntities?.searchResults || [];

    const newProperties = [...existingProperties.filter((prop) => prop.entity.urn !== deleteUrn)];

    client.writeQuery({
        query: GetSearchResultsForMultipleDocument,
        variables: {
            input: inputs,
        },
        data: {
            searchAcrossEntities: {
                ...searchAcrossEntities,
                total: newProperties.length,
                searchResults: newProperties,
            },
        },
    });
};
