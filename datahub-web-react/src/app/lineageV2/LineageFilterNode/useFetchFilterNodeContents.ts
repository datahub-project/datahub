import { PlatformFieldsFragment } from '../../../graphql/fragments.generated';
import { useAggregateAcrossEntitiesQuery } from '../../../graphql/search.generated';
import { AggregationMetadata } from '../../../types.generated';
import { ENTITY_SUB_TYPE_FILTER_NAME, FILTER_DELIMITER, PLATFORM_FILTER_NAME } from '../../searchV2/utils/constants';

export type PlatformAggregate = readonly [string, number, PlatformFieldsFragment];
export type SubtypeAggregate = readonly [string, number];

interface Return {
    platforms: PlatformAggregate[];
    subtypes: SubtypeAggregate[];
}

export default function useFetchFilterNodeContents(urns: string[]): Return {
    const { data } = useAggregateAcrossEntitiesQuery({
        fetchPolicy: 'cache-first',
        variables: {
            input: {
                query: '*',
                facets: [ENTITY_SUB_TYPE_FILTER_NAME, PLATFORM_FILTER_NAME],
                orFilters: [
                    {
                        and: [
                            {
                                field: 'urn',
                                values: urns,
                            },
                        ],
                    },
                ],
            },
        },
    });

    const platformAgg =
        data?.aggregateAcrossEntities?.facets?.find((facet) => facet.field === PLATFORM_FILTER_NAME)?.aggregations ||
        [];
    const platforms = platformAgg
        .filter((agg): agg is AggregationMetadata & { entity: PlatformFieldsFragment } => {
            return agg?.entity?.__typename === 'DataPlatform';
        })
        .map((agg) => [agg.value, agg.count, agg.entity] as const);
    platforms.sort(sortByCount);

    const subtypeAgg =
        data?.aggregateAcrossEntities?.facets?.find((facet) => facet.field === ENTITY_SUB_TYPE_FILTER_NAME)
            ?.aggregations || [];
    const subtypesMap = new Map(subtypeAgg.map((agg) => [agg.value, agg.count]));
    Array.from(subtypesMap).forEach(([filterValue, count]) => {
        if (filterValue.includes(FILTER_DELIMITER)) {
            const [platform] = filterValue.split(FILTER_DELIMITER);
            subtypesMap.set(platform, (subtypesMap.get(platform) || 0) - count);
        }
    });

    const subtypes = Array.from(subtypesMap).filter(([, count]) => count > 0);
    subtypes.sort(sortByCount);

    return { platforms, subtypes };
}

function sortByCount(a: readonly [string, number, any?], b: readonly [string, number, any?]) {
    return b[1] - a[1];
}
