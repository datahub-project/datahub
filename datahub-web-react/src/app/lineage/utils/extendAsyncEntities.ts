import { Dataset } from '../../../types.generated';
import { Direction, FetchedEntities } from '../types';
import getChildren from './getChildren';

export default function extendAsyncEntities(
    fetchedEntities: FetchedEntities,
    entity: Dataset,
    direction: Direction | null,
    fullyFetched = false,
): FetchedEntities {
    return {
        ...fetchedEntities,
        [entity.urn]: {
            urn: entity.urn,
            name: entity.name,
            type: entity.type,
            children: getChildren(entity, direction).map((child) => child.dataset.urn),
            fullyFetched,
        },
    };
}
