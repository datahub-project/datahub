import { EventType } from '@app/analytics';
import analytics from '@app/analytics/analytics';
import EntityRegistry from '@app/entity/EntityRegistry';
import { Direction } from '@app/lineage/types';

import { Entity, EntityType, LineageDirection } from '@types';

interface AnalyticsEventsProps {
    direction: LineageDirection;
    entitiesToAdd: Entity[];
    entitiesToRemove: Entity[];
    entityRegistry: EntityRegistry;
    entityType?: EntityType;
    entityPlatform?: string;
}

export function recordAnalyticsEvents({
    direction,
    entitiesToAdd,
    entitiesToRemove,
    entityRegistry,
    entityType,
    entityPlatform,
}: AnalyticsEventsProps) {
    entitiesToAdd.forEach((entityToAdd) => {
        const genericProps = entityRegistry.getGenericEntityProperties(entityToAdd.type, entityToAdd);
        analytics.event({
            type: EventType.ManuallyCreateLineageEvent,
            direction: directionFromLineageDirection(direction),
            sourceEntityType: entityType,
            sourceEntityPlatform: entityPlatform,
            destinationEntityType: entityToAdd.type,
            destinationEntityPlatform: genericProps?.platform?.name,
        });
    });
    entitiesToRemove.forEach((entityToRemove) => {
        const genericProps = entityRegistry.getGenericEntityProperties(entityToRemove.type, entityToRemove);
        analytics.event({
            type: EventType.ManuallyDeleteLineageEvent,
            direction: directionFromLineageDirection(direction),
            sourceEntityType: entityType,
            sourceEntityPlatform: entityPlatform,
            destinationEntityType: entityToRemove.type,
            destinationEntityPlatform: genericProps?.platform?.name,
        });
    });
}

function directionFromLineageDirection(lineageDirection: LineageDirection): Direction {
    return lineageDirection === LineageDirection.Upstream ? Direction.Upstream : Direction.Downstream;
}
