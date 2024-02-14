import { useAggregateAcrossEntitiesQuery } from '../../../graphql/search.generated';
import { AggregationMetadata } from '../../../types.generated';
import { PLATFORM_FILTER_NAME, TYPE_NAMES_FILTER_NAME } from '../../searchV2/utils/constants';
import { PlatformFieldsFragment } from '../../../graphql/fragments.generated';

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
                facets: [TYPE_NAMES_FILTER_NAME, PLATFORM_FILTER_NAME],
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
        .filter(
            (agg): agg is AggregationMetadata & { entity: PlatformFieldsFragment } =>
                agg?.entity?.__typename === 'DataPlatform',
        )
        .map((agg) => [agg.value, agg.count, agg.entity] as const);

    const subtypeAgg =
        data?.aggregateAcrossEntities?.facets?.find((facet) => facet.field === TYPE_NAMES_FILTER_NAME)?.aggregations ||
        [];
    const subtypes = subtypeAgg.map((agg) => [agg.value, agg.count] as const);

    return { platforms, subtypes };
}
