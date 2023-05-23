import { useAggregateAcrossEntitiesLazyQuery } from '../../../graphql/search.generated';
import { ORIGIN_FILTER_NAME, PLATFORM_FILTER_NAME } from '../utils/constants';
import { EntityType } from '../../../types.generated';
import useSidebarFilters from './useSidebarFilters';

type Props = {
    entityType: EntityType;
    environment?: string | null;
    platform?: string | null;
    facets: string[];
};

const useAggregationsQuery = ({ entityType, environment, platform, facets }: Props) => {
    const { query, orFilters, viewUrn } = useSidebarFilters({ environment, platform });

    const [getAggregations, { data: newData, previousData, loading, error }] = useAggregateAcrossEntitiesLazyQuery({
        fetchPolicy: 'cache-first',
    });

    const getAggregationsApi = () => {
        getAggregations({
            variables: {
                input: {
                    types: [entityType],
                    query,
                    orFilters,
                    viewUrn,
                    facets,
                },
            },
        });
    };

    const data = error ? null : newData ?? previousData;
    const loaded = !!data || !!error;

    const environmentAggregations =
        data?.aggregateAcrossEntities?.facets
            ?.find((facet) => facet.field === ORIGIN_FILTER_NAME)
            ?.aggregations.filter((aggregation) => aggregation.count) ?? [];

    const platformAggregations =
        data?.aggregateAcrossEntities?.facets
            ?.find((facet) => facet.field === PLATFORM_FILTER_NAME)
            ?.aggregations.filter((aggregation) => aggregation.count) ?? [];

    return [
        getAggregationsApi,
        {
            loading,
            loaded,
            error,
            environmentAggregations,
            platformAggregations,
        } as const,
    ] as const;
};

export default useAggregationsQuery;
