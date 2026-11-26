import { DOMAINS_FILTER_NAME, PLATFORM_FILTER_NAME } from '@app/searchV2/utils/constants';

import { DataPlatform, Domain, Entity, EntityType, FacetFilterInput, FacetMetadata, FilterOperator } from '@types';

export const METRICS_GROUP_BY = {
    SEMANTIC_MODEL: 'semantic_model',
    PLATFORM: 'platform',
    DOMAIN: 'domain',
} as const;

export type MetricsGroupByValue = (typeof METRICS_GROUP_BY)[keyof typeof METRICS_GROUP_BY];
export type GroupedMetricsMode = Exclude<MetricsGroupByValue, typeof METRICS_GROUP_BY.SEMANTIC_MODEL>;

export const UNASSIGNED_DOMAIN_GROUP_KEY = '__unassigned_domain__';

export function isMetricsGroupByValue(value: string): value is MetricsGroupByValue {
    return (
        value === METRICS_GROUP_BY.SEMANTIC_MODEL ||
        value === METRICS_GROUP_BY.PLATFORM ||
        value === METRICS_GROUP_BY.DOMAIN
    );
}

type MetricsGroupBase = {
    key: string;
    label: string;
};

export type MetricsGroup =
    | (MetricsGroupBase & {
          mode: typeof METRICS_GROUP_BY.PLATFORM;
          entity?: DataPlatform;
      })
    | (MetricsGroupBase & {
          mode: typeof METRICS_GROUP_BY.DOMAIN;
          entity?: Domain;
      });

type MetricsEntityGroupingData = {
    urn: string;
    platform?: DataPlatform | null;
    domain?: Domain | null;
};

type BuildMetricsGroupsOptions = {
    mode: GroupedMetricsMode;
    aggregations: FacetMetadata['aggregations'];
    unassignedCount: number;
    unassignedLabel: string;
    activeGroup?: MetricsGroup;
    getDisplayName: (entity: DataPlatform | Domain) => string;
};

export function buildMetricsGroupFilter(mode: GroupedMetricsMode, groupKey: string): FacetFilterInput {
    if (mode === METRICS_GROUP_BY.PLATFORM) {
        return { field: PLATFORM_FILTER_NAME, values: [groupKey] };
    }
    if (groupKey === UNASSIGNED_DOMAIN_GROUP_KEY) {
        return { field: DOMAINS_FILTER_NAME, condition: FilterOperator.Exists, negated: true };
    }
    return { field: DOMAINS_FILTER_NAME, values: [groupKey] };
}

export function getMetricsGroupField(mode: GroupedMetricsMode): string {
    return mode === METRICS_GROUP_BY.PLATFORM ? PLATFORM_FILTER_NAME : DOMAINS_FILTER_NAME;
}

function isDataPlatform(entity: Entity | null | undefined): entity is DataPlatform {
    return entity?.type === EntityType.DataPlatform;
}

function isDomain(entity: Entity | null | undefined): entity is Domain {
    return entity?.type === EntityType.Domain;
}

export function sumFacetAggregationCounts(facet?: FacetMetadata): number {
    return (facet?.aggregations ?? []).reduce((total, aggregation) => total + aggregation.count, 0);
}

export function buildMetricsGroups({
    mode,
    aggregations,
    unassignedCount,
    unassignedLabel,
    activeGroup,
    getDisplayName,
}: BuildMetricsGroupsOptions): MetricsGroup[] {
    const groups = aggregations
        .filter((aggregation) => aggregation.count > 0 && !!aggregation.value)
        .map<MetricsGroup>((aggregation) => {
            if (mode === METRICS_GROUP_BY.PLATFORM) {
                const entity = isDataPlatform(aggregation.entity) ? aggregation.entity : undefined;
                return {
                    mode,
                    key: aggregation.value,
                    label: entity ? getDisplayName(entity) : aggregation.value,
                    entity,
                };
            }

            const entity = isDomain(aggregation.entity) ? aggregation.entity : undefined;
            return {
                mode,
                key: aggregation.value,
                label: entity ? getDisplayName(entity) : aggregation.value,
                entity,
            };
        });

    if (mode === METRICS_GROUP_BY.DOMAIN && (unassignedCount > 0 || activeGroup?.key === UNASSIGNED_DOMAIN_GROUP_KEY)) {
        groups.push({
            mode,
            key: UNASSIGNED_DOMAIN_GROUP_KEY,
            label: unassignedLabel,
        });
    }

    if (activeGroup && !groups.some((group) => group.key === activeGroup.key)) {
        groups.push(activeGroup);
    }

    return groups.sort((left, right) => left.label.localeCompare(right.label));
}

export function resolveActiveMetricsGroup(
    entityData: MetricsEntityGroupingData | null,
    selectedUrn: string | null,
    mode: GroupedMetricsMode,
    unassignedLabel: string,
    getDisplayName: (entity: DataPlatform | Domain) => string,
): MetricsGroup | undefined {
    if (!entityData || entityData.urn !== selectedUrn) return undefined;

    if (mode === METRICS_GROUP_BY.PLATFORM) {
        if (!entityData.platform) return undefined;
        return {
            mode,
            key: entityData.platform.urn,
            label: getDisplayName(entityData.platform),
            entity: entityData.platform,
        };
    }

    if (entityData.domain) {
        return {
            mode,
            key: entityData.domain.urn,
            label: getDisplayName(entityData.domain),
            entity: entityData.domain,
        };
    }

    return {
        mode,
        key: UNASSIGNED_DOMAIN_GROUP_KEY,
        label: unassignedLabel,
    };
}
