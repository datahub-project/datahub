import { useMemo } from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { SUPPORTED_STRUCTURED_PROPERTY_VALUE_TYPES } from '@app/entityV2/summary/properties/constants';
import { AssetProperty, PropertyType } from '@app/entityV2/summary/properties/types';
import { getStructuredPropertiesSearchInputs, isStructuredProperty } from '@app/govern/structuredProperties/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';

export default function useStructuredProperties(query: string | undefined) {
    const { entityType } = useEntityData();
    const preprocessedQuery = (query ?? '').trim();

    const entityRegistry = useEntityRegistry();

    // AUTOCOMPLETE API (Structured properties isn't searchable right now)

    // const inputs: AutoCompleteInput = {
    //     type: EntityType.StructuredProperty,
    //     query: preprocessedQuery || '*',
    //     limit: MAX_PROPERTIES,
    //     field: 'displayName',
    //     filters: [
    //         getEntityTypesPropertyFilter(entityRegistry, false, entityType),
    //         getNotHiddenPropertyFilter(),
    //         getValueTypeFilter(SUPPORTED_STRUCTURED_PROPERTY_VALUE_TYPES),
    //     ],
    // };

    // const { data, loading } = useGetAutoCompleteResultsQuery({
    //     variables: {
    //         input: inputs,
    //     },
    // });

    // const structuredProperties: AssetProperty[] = useMemo(() => {
    //     return (
    //         ((data?.autoComplete?.entities ?? [])
    //             .filter(isStructuredProperty)
    //             ?.map((structuredProperty) => ({
    //                 key: structuredProperty.urn,
    //                 name: structuredProperty.definition.displayName ?? '',
    //                 type: PropertyType.StructuredProperty,
    //                 structuredPropertyUrn: structuredProperty.urn,
    //             }))
    //             .filter((property) => !!property.name) as AssetProperty[]) ?? []
    //     );
    // }, [data]);

    // SEARCH API

    const { data, loading } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: getStructuredPropertiesSearchInputs(entityRegistry, entityType, '', preprocessedQuery),
        },
        fetchPolicy: 'cache-first',
    });

    const structuredProperties: AssetProperty[] = useMemo(() => {
        return (
            ((data?.searchAcrossEntities?.searchResults ?? [])
                .map((result) => result.entity)
                .filter(isStructuredProperty)
                .filter((property) =>
                    SUPPORTED_STRUCTURED_PROPERTY_VALUE_TYPES.includes(property.definition.valueType.urn),
                )
                ?.map((structuredProperty) => ({
                    key: structuredProperty.urn,
                    name: structuredProperty.definition.displayName ?? '',
                    type: PropertyType.StructuredProperty,
                    structuredPropertyUrn: structuredProperty.urn,
                }))
                .filter((property) => !!property.name) as AssetProperty[]) ?? []
        );
    }, [data]);

    return { structuredProperties, loading };
}
