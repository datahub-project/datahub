import { Direction, EntityAndType } from '../types';

export default function getChildren(entityAndType: EntityAndType, direction: Direction | null): Array<EntityAndType> {
    if (direction === Direction.Upstream) {
        return (
            entityAndType.entity.upstreamLineage?.entities?.map(
                (entity) =>
                    ({
                        type: entity?.entity?.type,
                        entity: entity?.entity,
                    } as EntityAndType),
            ) || []
        );
    }
    if (direction === Direction.Downstream) {
        return (
            entityAndType.entity.downstreamLineage?.entities?.map(
                (entity) =>
                    ({
                        type: entity?.entity?.type,
                        entity: entity?.entity,
                    } as EntityAndType),
            ) || []
        );
    }

    return [];
}
