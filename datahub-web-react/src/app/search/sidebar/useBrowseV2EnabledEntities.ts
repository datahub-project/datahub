import { EntityType, FacetMetadata } from '../../../types.generated';
import { GLOSSARY_ENTITY_TYPES } from '../../entity/shared/constants';
import { useEntityRegistry } from '../../useEntityRegistry';
import { ENTITY_FILTER_NAME } from '../utils/constants';

const useBrowseV2EnabledEntities = (facets?: Array<FacetMetadata> | null) => {
    const registry = useEntityRegistry();

    return facets
        ?.find((facet) => facet.field === ENTITY_FILTER_NAME)
        ?.aggregations.filter(({ count, value }) => {
            const entityType = value as EntityType;
            return (
                count && registry.getEntity(entityType).isBrowseEnabled() && !GLOSSARY_ENTITY_TYPES.includes(entityType)
            );
        })
        .sort((a, b) => a.value.localeCompare(b.value));
};

export default useBrowseV2EnabledEntities;
