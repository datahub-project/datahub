import { extractTypeFromUrn } from '@app/entity/shared/utils';

import { EntityType, LineageDirection } from '@types';

export function getValidEntityTypes(lineageDirection: LineageDirection, entityType?: EntityType) {
    if (lineageDirection === LineageDirection.Upstream) {
        switch (entityType) {
            case EntityType.Dataset:
                return [EntityType.Dataset, EntityType.DataJob, EntityType.Metric];
            case EntityType.Chart:
                return [EntityType.Dataset, EntityType.Metric];
            case EntityType.Dashboard:
                return [EntityType.Chart, EntityType.Dataset, EntityType.Metric];
            case EntityType.DataJob:
                return [EntityType.DataJob, EntityType.Dataset];
            case EntityType.Metric:
                // updateLineage does not write metricUpstreams; do not offer a no-op picker
                return [];
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
            case EntityType.Metric:
                return [EntityType.Dataset, EntityType.Chart, EntityType.Dashboard];
            default:
                console.warn('Unexpected entity type to get valid downstream entity types for');
                return [];
        }
    }
}

/** Keep the current-list and add/remove payload on types updateLineage can persist. */
export function filterManualLineageUrns(urns: Iterable<string>, validTypes: EntityType[]): string[] {
    const allowed = new Set(validTypes);
    return Array.from(urns).filter((urn) => allowed.has(extractTypeFromUrn(urn)));
}
