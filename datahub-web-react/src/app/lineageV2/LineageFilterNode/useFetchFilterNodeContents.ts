import { DBT_URN } from '@app/ingest/source/builder/constants';
import { useGetLineageTimeParams } from '@app/lineage/utils/useGetLineageTimeParams';
import { LineageNodesContext } from '@app/lineageV2/common';
import { DEGREE_FILTER_NAME } from '@app/search/utils/constants';
import { useContext } from 'react';
import { PlatformFieldsFragment } from '../../../graphql/fragments.generated';
import { useAggregateAcrossLineageQuery } from '../../../graphql/search.generated';
import { AggregationMetadata, AndFilterInput, EntityType, LineageDirection } from '../../../types.generated';
import {
    ENTITY_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    FILTER_DELIMITER,
    PLATFORM_FILTER_NAME,
} from '../../searchV2/utils/constants';

export type PlatformAggregate = readonly [string, number, PlatformFieldsFragment];
export type SubtypeAggregate = readonly [string, number];

interface Return {
    platforms: PlatformAggregate[];
    subtypes: SubtypeAggregate[];
    total?: number;
}

export default function useFetchFilterNodeContents(parent: string, direction: LineageDirection, skip: boolean): Return {
    const { startTimeMillis, endTimeMillis } = useGetLineageTimeParams();

    const orFilters = useGetOrFilters();
    const { data } = useAggregateAcrossLineageQuery({
        skip,
        fetchPolicy: 'cache-first',
        variables: {
            input: {
                urn: parent,
                query: '*',
                direction,
                orFilters,
                lineageFlags: {
                    startTimeMillis,
                    endTimeMillis,
                    ignoreAsHops: [
                        {
                            entityType: EntityType.Dataset,
                            platforms: [DBT_URN],
                        },
                        { entityType: EntityType.DataJob },
                    ],
                },
                searchFlags: {
                    skipCache: true, // TODO: Figure how to get around not needing this
                },
            },
        },
    });

    const platformAgg =
        data?.searchAcrossLineage?.facets?.find((facet) => facet.field === PLATFORM_FILTER_NAME)?.aggregations || [];
    const platforms = platformAgg
        .filter((agg): agg is AggregationMetadata & { entity: PlatformFieldsFragment } => {
            return agg?.entity?.__typename === 'DataPlatform' && !!agg.count;
        })
        .map((agg) => [agg.value, agg.count, agg.entity] as const);
    platforms.sort(sortByCount);

    const subtypeAgg =
        data?.searchAcrossLineage?.facets?.find((facet) => facet.field === ENTITY_SUB_TYPE_FILTER_NAME)?.aggregations ||
        [];
    const subtypesMap = new Map(subtypeAgg.map((agg) => [agg.value, agg.count]));
    Array.from(subtypesMap).forEach(([filterValue, count]) => {
        if (filterValue.includes(FILTER_DELIMITER)) {
            const [platform] = filterValue.split(FILTER_DELIMITER);
            subtypesMap.set(platform, (subtypesMap.get(platform) || 0) - count);
        }
    });

    const subtypes = Array.from(subtypesMap).filter(([, count]) => count > 0);
    subtypes.sort(sortByCount);

    return { total: data?.searchAcrossLineage?.total, platforms, subtypes };
}

/**
 * Returns or filters for getting related entities at depth 1, potentially filtering out transformations.
 * Transformations are defined as (type = dataset ^ platform = dbt) v (type = datajob).
 * We can transform this into the correct format via logical equivalence:
 * (depth = 1) ^ ~((type = dataset ^ platform = dbt) v (type = datajob))
 * = (depth = 1) ^ ((type != dataset v platform != dbt) ^ (type != datajob)) // De Morgan's Law
 * = (depth = 1 ^ type != dataset ^ type != datajob) v (depth = 1 ^ platform != dbt ^ type != datajob) // Distributive Law
 */
function useGetOrFilters(): AndFilterInput[] {
    const { hideTransformations } = useContext(LineageNodesContext);

    if (!hideTransformations) {
        return [{ and: [{ field: DEGREE_FILTER_NAME, values: ['1'] }] }];
    }
    return [
        {
            and: [
                { field: DEGREE_FILTER_NAME, values: ['1'] },
                {
                    field: ENTITY_FILTER_NAME,
                    values: [EntityType.Dataset, EntityType.DataJob],
                    negated: true,
                },
            ],
        },
        {
            and: [
                { field: DEGREE_FILTER_NAME, values: ['1'] },
                {
                    field: ENTITY_FILTER_NAME,
                    values: [EntityType.DataJob],
                    negated: true,
                },
                { field: PLATFORM_FILTER_NAME, values: [DBT_URN], negated: true },
            ],
        },
    ];
}

function sortByCount(a: readonly [string, number, any?], b: readonly [string, number, any?]) {
    return b[1] - a[1];
}
