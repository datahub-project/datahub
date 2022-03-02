import * as React from 'react';
import { useMemo } from 'react';
import { EntityType, SearchResult } from '../../../../types.generated';
import { useGetEntitySearchResults } from '../../../../utils/customGraphQL/useGetEntitySearchResults';
import RelatedEntityResults from '../../../shared/entitySearch/RelatedEntityResults';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { useEntityData } from '../../shared/EntityContext';

export default function GlossaryRelatedEntity() {
    const { entityData }: any = useEntityData();
    const entityRegistry = useEntityRegistry();
    const searchTypes = entityRegistry.getSearchEntityTypes();
    searchTypes.splice(searchTypes.indexOf(EntityType.GlossaryTerm), 1);
    const glossaryTermHierarchicalName = entityData?.hierarchicalName;
    const entitySearchResult = useGetEntitySearchResults(
        {
            query: `glossaryTerms:(${glossaryTermHierarchicalName}) OR fieldGlossaryTerms:(${glossaryTermHierarchicalName}) OR editedFieldGlossaryTerms:(${glossaryTermHierarchicalName})`,
        },
        searchTypes,
    );

    const entitySearchForDetails = useMemo(() => {
        const filteredSearchResult: {
            [key in EntityType]?: Array<SearchResult>;
        } = {};

        Object.keys(entitySearchResult).forEach((type) => {
            const entities = entitySearchResult[type].data?.search?.searchResults;
            if (entities && entities.length > 0) {
                filteredSearchResult[type] = entities;
            }
        });

        return filteredSearchResult;
    }, [entitySearchResult]);

    return <RelatedEntityResults searchResult={entitySearchForDetails} />;
}
