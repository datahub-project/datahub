import { EntityType } from '../../../types.generated';
import { Direction, EntityAndType } from '../types';

export default function getChildren(entityAndType: EntityAndType, direction: Direction | null): Array<EntityAndType> {
    if (direction === Direction.Upstream) {
        if (entityAndType.type === EntityType.Dataset) {
            return (
                entityAndType.entity.upstreamLineage?.upstreams.map((upstream) => ({
                    type: EntityType.Dataset,
                    entity: upstream.dataset,
                })) || []
            );
        }
        if (entityAndType.type === EntityType.Chart) {
            return (
                entityAndType.entity.info?.inputs?.map((dataset) => ({
                    type: EntityType.Dataset,
                    entity: dataset,
                })) || []
            );
        }
        if (entityAndType.type === EntityType.Dashboard) {
            return (
                entityAndType.entity.info?.charts.map((chart) => ({
                    type: EntityType.Chart,
                    entity: chart,
                })) || []
            );
        }
    }

    if (direction === Direction.Downstream) {
        if (entityAndType.type === EntityType.Dataset) {
            return (
                entityAndType.entity.downstreamLineage?.downstreams.map((downstreams) => ({
                    type: EntityType.Dataset,
                    entity: downstreams.dataset,
                })) || []
            );
        }
    }

    return [];
}
