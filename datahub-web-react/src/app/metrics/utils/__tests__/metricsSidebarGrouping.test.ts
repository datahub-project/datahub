import {
    METRICS_GROUP_BY,
    UNASSIGNED_DOMAIN_GROUP_KEY,
    buildMetricsGroupFilter,
    buildMetricsGroups,
    getMetricsGroupField,
    resolveActiveMetricsGroup,
    sumFacetAggregationCounts,
} from '@app/metrics/utils/metricsSidebarGrouping';

import { DataPlatform, Domain, EntityType, FacetMetadata, FilterOperator } from '@types';

const platform = {
    __typename: 'DataPlatform',
    urn: 'urn:li:dataPlatform:snowflake',
    type: EntityType.DataPlatform,
} as DataPlatform;

const domain = {
    __typename: 'Domain',
    urn: 'urn:li:domain:marketing',
    type: EntityType.Domain,
} as Domain;

describe('metrics sidebar grouping', () => {
    it('builds platform and assigned-domain filters', () => {
        expect(buildMetricsGroupFilter(METRICS_GROUP_BY.PLATFORM, 'urn:li:dataPlatform:snowflake')).toEqual({
            field: 'platform',
            values: ['urn:li:dataPlatform:snowflake'],
        });
        expect(buildMetricsGroupFilter(METRICS_GROUP_BY.DOMAIN, 'urn:li:domain:marketing')).toEqual({
            field: 'domains',
            values: ['urn:li:domain:marketing'],
        });
    });

    it('builds a not-exists filter for the unassigned domain group', () => {
        expect(buildMetricsGroupFilter(METRICS_GROUP_BY.DOMAIN, UNASSIGNED_DOMAIN_GROUP_KEY)).toEqual({
            field: 'domains',
            condition: FilterOperator.Exists,
            negated: true,
        });
    });

    it('maps grouped modes to indexed facet fields', () => {
        expect(getMetricsGroupField(METRICS_GROUP_BY.PLATFORM)).toBe('platform');
        expect(getMetricsGroupField(METRICS_GROUP_BY.DOMAIN)).toBe('domains');
    });

    it('maps facet aggregations to sorted groups without retaining unused counts', () => {
        const aggregations = [
            { value: platform.urn, count: 2, entity: platform },
            { value: 'urn:li:dataPlatform:empty', count: 0 },
        ] as FacetMetadata['aggregations'];

        expect(
            buildMetricsGroups({
                mode: METRICS_GROUP_BY.PLATFORM,
                aggregations,
                unassignedCount: 0,
                unassignedLabel: 'Unassigned',
                getDisplayName: () => 'Snowflake',
            }),
        ).toEqual([
            {
                mode: METRICS_GROUP_BY.PLATFORM,
                key: platform.urn,
                label: 'Snowflake',
                entity: platform,
            },
        ]);
    });

    it('adds unassigned and active domain groups when facets omit them', () => {
        const activeGroup = {
            mode: METRICS_GROUP_BY.DOMAIN,
            key: domain.urn,
            label: 'Marketing',
            entity: domain,
        } as const;

        expect(
            buildMetricsGroups({
                mode: METRICS_GROUP_BY.DOMAIN,
                aggregations: [],
                unassignedCount: 3,
                unassignedLabel: 'Unassigned',
                activeGroup,
                getDisplayName: () => 'Marketing',
            }),
        ).toEqual([
            activeGroup,
            {
                mode: METRICS_GROUP_BY.DOMAIN,
                key: UNASSIGNED_DOMAIN_GROUP_KEY,
                label: 'Unassigned',
            },
        ]);
    });

    it('only resolves an active group for the entity selected in the route', () => {
        const entityData = {
            urn: 'urn:li:metric:active',
            platform,
            domain: null,
        };

        expect(
            resolveActiveMetricsGroup(
                entityData,
                'urn:li:metric:other',
                METRICS_GROUP_BY.PLATFORM,
                'Unassigned',
                () => 'Snowflake',
            ),
        ).toBeUndefined();
        expect(
            resolveActiveMetricsGroup(
                entityData,
                entityData.urn,
                METRICS_GROUP_BY.DOMAIN,
                'Unassigned',
                () => 'unused',
            ),
        ).toEqual({
            mode: METRICS_GROUP_BY.DOMAIN,
            key: UNASSIGNED_DOMAIN_GROUP_KEY,
            label: 'Unassigned',
        });
    });

    it('sums facet counts for the unassigned group', () => {
        const facet = {
            aggregations: [
                { value: EntityType.Metric, count: 4 },
                { value: EntityType.SemanticModel, count: 2 },
            ],
        } as FacetMetadata;

        expect(sumFacetAggregationCounts(facet)).toBe(6);
        expect(sumFacetAggregationCounts()).toBe(0);
    });
});
