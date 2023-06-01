import { useAggregateAcrossEntitiesQuery } from '../../../graphql/search.generated';
import { ENTITY_FILTER_NAME, ORIGIN_FILTER_NAME, PLATFORM_FILTER_NAME } from '../utils/constants';
import { useEntityType } from './BrowseContext';
import useSidebarFilters from './useSidebarFilters';

type Props = {
    facets: string[];
    skip: boolean;
};

// todo - maybe pull this out of context and just accept props?
const useAggregationsQuery = ({ facets, skip }: Props) => {
    const entityType = useEntityType();
    const filters = useSidebarFilters();

    const {
        data: newData,
        previousData,
        loading,
        error,
    } = useAggregateAcrossEntitiesQuery({
        skip,
        fetchPolicy: 'cache-first',
        variables: {
            input: {
                types: [entityType],
                facets,
                ...filters,
            },
        },
    });

    const data = error ? null : newData ?? previousData;
    const loaded = !!data || !!error;

    const entityAggregations =
        data?.aggregateAcrossEntities?.facets
            ?.find((facet) => facet.field === ENTITY_FILTER_NAME)
            ?.aggregations.filter((aggregation) => aggregation.count) ?? [];

    const environmentAggregations =
        data?.aggregateAcrossEntities?.facets
            ?.find((facet) => facet.field === ORIGIN_FILTER_NAME)
            ?.aggregations.filter((aggregation) => aggregation.count) ?? [];

    const platformAggregations =
        data?.aggregateAcrossEntities?.facets
            ?.find((facet) => facet.field === PLATFORM_FILTER_NAME)
            ?.aggregations.filter((aggregation) => aggregation.count) ?? [];

    return {
        loading,
        loaded,
        error,
        entityAggregations,
        environmentAggregations,
        platformAggregations,
    } as const;
};

export default useAggregationsQuery;
