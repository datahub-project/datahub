import { useMemo } from 'react';

import {
    GroupedMetricsMode,
    METRICS_GROUP_BY,
    MetricsGroup,
    buildMetricsGroups,
    getMetricsGroupField,
    sumFacetAggregationCounts,
} from '@app/metrics/utils/metricsSidebarGrouping';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useAggregateAcrossEntitiesQuery } from '@graphql/search.generated';
import { EntityType, FilterOperator } from '@types';

const GROUP_FACET_MAX = 100;
const ENTITY_TYPE_FACET = '_entityType';

export default function useMetricsGroupRoots(
    mode: GroupedMetricsMode,
    unassignedLabel: string,
    activeGroup?: MetricsGroup,
): { groups: MetricsGroup[]; loading: boolean } {
    const entityRegistry = useEntityRegistry();
    const field = getMetricsGroupField(mode);

    const { data, previousData, loading } = useAggregateAcrossEntitiesQuery({
        fetchPolicy: 'network-only',
        variables: {
            input: {
                types: [EntityType.Metric, EntityType.SemanticModel],
                query: '*',
                facets: [field],
                searchFlags: { maxAggValues: GROUP_FACET_MAX },
            },
        },
    });

    const {
        data: unassignedData,
        previousData: previousUnassignedData,
        loading: unassignedLoading,
    } = useAggregateAcrossEntitiesQuery({
        skip: mode !== METRICS_GROUP_BY.DOMAIN,
        fetchPolicy: 'network-only',
        variables: {
            input: {
                types: [EntityType.Metric, EntityType.SemanticModel],
                query: '*',
                facets: [ENTITY_TYPE_FACET],
                orFilters: [
                    {
                        and: [
                            {
                                field: getMetricsGroupField(METRICS_GROUP_BY.DOMAIN),
                                condition: FilterOperator.Exists,
                                negated: true,
                            },
                        ],
                    },
                ],
            },
        },
    });

    const groups = useMemo(() => {
        const resolvedData = data ?? previousData;
        const facet = (resolvedData?.aggregateAcrossEntities?.facets ?? []).find((item) => item.field === field);
        const resolvedUnassignedData = unassignedData ?? previousUnassignedData;
        const unassignedFacet = (resolvedUnassignedData?.aggregateAcrossEntities?.facets ?? []).find(
            (item) => item.field === ENTITY_TYPE_FACET,
        );

        return buildMetricsGroups({
            mode,
            aggregations: facet?.aggregations ?? [],
            unassignedCount: sumFacetAggregationCounts(unassignedFacet),
            unassignedLabel,
            activeGroup,
            getDisplayName: (entity) => entityRegistry.getDisplayName(entity.type, entity),
        });
    }, [
        activeGroup,
        data,
        entityRegistry,
        field,
        mode,
        previousData,
        previousUnassignedData,
        unassignedData,
        unassignedLabel,
    ]);

    return {
        groups,
        loading: loading || (mode === METRICS_GROUP_BY.DOMAIN && unassignedLoading),
    };
}
