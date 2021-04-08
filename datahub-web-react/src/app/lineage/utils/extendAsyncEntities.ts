import { Dataset } from '../../../types.generated';
import { Direction, FetchedEntities } from '../types';
import getChildren from './getChildren';

export default function extendAsyncEntities(
    fetchedEntities: FetchedEntities,
    entity: Dataset,
    fullyFetched = false,
): FetchedEntities {
    if (fetchedEntities[entity.urn]?.fullyFetched) {
        return fetchedEntities;
    }
    return {
        ...fetchedEntities,
        [entity.urn]: {
            urn: entity.urn,
            name: entity.name,
            type: entity.type,
            icon: entity.platform.info?.logoUrl || undefined,
            upstreamChildren: getChildren(entity, Direction.Upstream).map((child) => child.dataset.urn),
            downstreamChildren: getChildren(entity, Direction.Downstream).map((child) => child.dataset.urn),
            fullyFetched,
        },
    };
}
