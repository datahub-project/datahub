import { DBT_URN } from '@app/ingest/source/builder/constants';
import { useGetLineageTimeParams } from '@app/lineage/utils/useGetLineageTimeParams';
import { LineageNodesContext } from '@app/lineageV2/common';
import computeOrFilters from '@app/lineageV2/LineageFilterNode/computeOrFilters';
import { DEGREE_FILTER_NAME } from '@app/search/utils/constants';
import { useContext } from 'react';
import { PlatformFieldsFragment } from '../../../graphql/fragments.generated';
import { useAggregateAcrossLineageQuery } from '../../../graphql/search.generated';
import { AggregationMetadata, EntityType, LineageDirection } from '../../../types.generated';
import { ENTITY_SUB_TYPE_FILTER_NAME, FILTER_DELIMITER, PLATFORM_FILTER_NAME } from '../../searchV2/utils/constants';

export type PlatformAggregate = readonly [string, number, PlatformFieldsFragment];
export type SubtypeAggregate = readonly [string, number];

interface Return {
    platforms: PlatformAggregate[];
    subtypes: SubtypeAggregate[];
    total?: number;
}

export default function useFetchFilterNodeContents(parent: string, direction: LineageDirection, skip: boolean): Return {
    const { startTimeMillis, endTimeMillis } = useGetLineageTimeParams();
    const { hideTransformations, showDataProcessInstances } = useContext(LineageNodesContext);

    const orFilters = computeOrFilters(
        [{ field: DEGREE_FILTER_NAME, values: ['1'] }],
        hideTransformations,
        showDataProcessInstances,
    );
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

function sortByCount(a: readonly [string, number, any?], b: readonly [string, number, any?]) {
    return b[1] - a[1];
}
