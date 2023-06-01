import { useAggregateAcrossEntitiesQuery } from '../../../graphql/search.generated';
import { EntityType } from '../../../types.generated';
import { GLOSSARY_ENTITY_TYPES } from '../../entity/shared/constants';
import { useEntityRegistry } from '../../useEntityRegistry';
import { ENTITY_FILTER_NAME, ORIGIN_FILTER_NAME, PLATFORM_FILTER_NAME } from '../utils/constants';
import { useMaybeEntityType } from './BrowseContext';
import useSidebarFilters from './useSidebarFilters';

type Props = {
    facets: string[];
    skip: boolean;
};

const useAggregationsQuery = ({ facets, skip }: Props) => {
    const registry = useEntityRegistry();
    const entityType = useMaybeEntityType();
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
                types: entityType ? [entityType] : null,
                facets,
                ...filters,
            },
        },
    });

    const data = error ? null : newData ?? previousData;
    const loaded = !!data || !!error;

    const entityAggregations = data?.aggregateAcrossEntities?.facets
        ?.find((facet) => facet.field === ENTITY_FILTER_NAME)
        ?.aggregations.filter(({ count, value }) => {
            const type = value as EntityType;
            return count && registry.getEntity(type).isBrowseEnabled() && !GLOSSARY_ENTITY_TYPES.includes(type);
        })
        .sort((a, b) => a.value.localeCompare(b.value));

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
