import { FeildFacetState } from '@src/app/searchV2/filtersV2/types';
import { Entity, EntityType } from '@src/types.generated';
import { useMemo } from 'react';
import { BaseEntitySelectOption } from '../types';
import useConvertEntitiesToOptions from './useEntitiesToOptions';
import useAutocompleteResults from './useAutocompleteResults';

const mergeEntityArrays = (arrayA: Entity[], arrayB: Entity[]): Entity[] => {
    const urnsFromArrayB = arrayB.map((entity) => entity.urn);

    return [...arrayA.filter((entity) => !urnsFromArrayB.includes(entity.urn)), ...arrayB];
};

export default function useOptions(
    appliedEntities: Entity[],
    facetState: FeildFacetState | undefined,
    query: string,
    entityTypes: EntityType[],
    entityRender: (entity: Entity) => React.ReactNode,
): BaseEntitySelectOption[] {
    const convertEntiteisToOptions = useConvertEntitiesToOptions();

    const { data: searchResponse, loading: searchResponseLoading } = useAutocompleteResults(query, entityTypes);

    const entitiesFromFacetState = useMemo(
        () =>
            (facetState?.facet?.aggregations ?? [])
                .filter((aggregation) => aggregation.count > 0)
                .map((aggregation) => aggregation.entity)
                .filter((entity): entity is Entity => !!entity),
        [facetState],
    );
    const entitiesFromSearchResponse = useMemo(
        () =>
            searchResponse?.autoCompleteForMultiple?.suggestions.map((suggestion) => suggestion.entities).flat() ?? [],
        [searchResponse],
    );

    const mergedEntities = useMemo(() => {
        let entities: Entity[] = mergeEntityArrays(appliedEntities, entitiesFromFacetState);

        if (query !== '' && !searchResponseLoading) {
            entities = mergeEntityArrays(entities, entitiesFromSearchResponse);
        }

        return entities;
    }, [appliedEntities, entitiesFromFacetState, entitiesFromSearchResponse, searchResponseLoading, query]);

    const options = useMemo(() => {
        return convertEntiteisToOptions(mergedEntities, entityRender);
    }, [mergedEntities, convertEntiteisToOptions, entityRender]);

    return options;
}
