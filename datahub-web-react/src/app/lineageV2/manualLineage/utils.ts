import { EntityType, LineageDirection } from '../../../types.generated';

export function getValidEntityTypes(lineageDirection: LineageDirection, entityType?: EntityType) {
    if (lineageDirection === LineageDirection.Upstream) {
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
