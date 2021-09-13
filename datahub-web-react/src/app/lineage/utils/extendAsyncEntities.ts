import EntityRegistry from '../../entity/EntityRegistry';
import { EntityAndType, FetchedEntities } from '../types';

export default function extendAsyncEntities(
    fetchedEntities: FetchedEntities,
    entityRegistry: EntityRegistry,
    entityAndType: EntityAndType,
    fullyFetched = false,
): FetchedEntities {
    if (fetchedEntities[entityAndType.entity.urn]?.fullyFetched) {
        return fetchedEntities;
    }

    const lineageVizConfig = entityRegistry.getLineageVizConfig(entityAndType.type, entityAndType.entity);

    if (!lineageVizConfig) return fetchedEntities;

    return {
        ...fetchedEntities,
        [entityAndType.entity.urn]: {
            ...lineageVizConfig,
            fullyFetched,
        },
    };
}
