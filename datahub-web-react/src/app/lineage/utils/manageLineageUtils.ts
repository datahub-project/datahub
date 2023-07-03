import { Entity, EntityType, LineageEdge } from '../../../types.generated';
import analytics, { EventType } from '../../analytics';
import EntityRegistry from '../../entity/EntityRegistry';
import { Direction } from '../types';

interface AnalyticsEventsProps {
    lineageDirection: Direction;
    entitiesToAdd: Entity[];
    entitiesToRemove: Entity[];
    entityRegistry: EntityRegistry;
    entityType?: EntityType;
    entityPlatform?: string;
}

export function recordAnalyticsEvents({
    lineageDirection,
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
            direction: lineageDirection,
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
            direction: lineageDirection,
            sourceEntityType: entityType,
            sourceEntityPlatform: entityPlatform,
            destinationEntityType: entityToRemove.type,
            destinationEntityPlatform: genericProps?.platform?.name,
        });
    });
}

export function buildUpdateLineagePayload(
    lineageDirection: Direction,
    entitiesToAdd: Entity[],
    entitiesToRemove: Entity[],
    entityUrn: string,
) {
    let edgesToAdd: LineageEdge[] = [];
    let edgesToRemove: LineageEdge[] = [];

    if (lineageDirection === Direction.Upstream) {
        edgesToAdd = entitiesToAdd.map((entity) => ({ upstreamUrn: entity.urn, downstreamUrn: entityUrn }));
        edgesToRemove = entitiesToRemove.map((entity) => ({ upstreamUrn: entity.urn, downstreamUrn: entityUrn }));
    }
    if (lineageDirection === Direction.Downstream) {
        edgesToAdd = entitiesToAdd.map((entity) => ({ upstreamUrn: entityUrn, downstreamUrn: entity.urn }));
        edgesToRemove = entitiesToRemove.map((entity) => ({ upstreamUrn: entityUrn, downstreamUrn: entity.urn }));
    }

    return { edgesToAdd, edgesToRemove };
}

export function getValidEntityTypes(lineageDirection: Direction, entityType?: EntityType) {
    if (lineageDirection === Direction.Upstream) {
        switch (entityType) {
            case EntityType.Dataset:
                return [EntityType.Dataset, EntityType.DataJob];
            case EntityType.Chart:
                return [EntityType.Dataset];
            case EntityType.Dashboard:
                return [EntityType.Chart, EntityType.Dataset];
            case EntityType.DataJob:
                return [EntityType.DataJob, EntityType.Dataset];
            default:
                console.warn('Unexpected entity type to get valid upstream entity types for');
                return [];
        }
    } else {
        switch (entityType) {
            case EntityType.Dataset:
                return [EntityType.Dataset, EntityType.Chart, EntityType.Dashboard, EntityType.DataJob];
            case EntityType.Chart:
                return [EntityType.Dashboard];
            case EntityType.Dashboard:
                console.warn('There are no valid lineage entities downstream of Dashboard entities');
                return [];
            case EntityType.DataJob:
                return [EntityType.DataJob, EntityType.Dataset];
            default:
                console.warn('Unexpected entity type to get valid downstream entity types for');
                return [];
        }
    }
}
