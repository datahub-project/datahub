import { useEntityData } from '@app/entity/shared/EntityContext';
import { getAndFilters } from '@app/entityV2/shared/tabs/Dataset/Queries/utils/filterQueries';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { useAggregateAcrossEntitiesQuery } from '@graphql/search.generated';
import { EntityType, FacetFilterInput, QuerySource } from '@types';

interface Props {
    selectedColumnsFilter: FacetFilterInput;
    selectedUsersFilter: FacetFilterInput;
}

export default function useUsersFilter({ selectedColumnsFilter, selectedUsersFilter }: Props) {
    const entityRegistry = useEntityRegistryV2();
    const { entityData } = useEntityData();
    const entityUrn = entityData?.urn;
    const siblingUrn = entityData?.siblingsSearch?.searchResults?.[0]?.entity?.urn;

    const values = siblingUrn ? [entityUrn as string, siblingUrn] : [entityUrn as string];
    const entityFilter = { field: 'entities', values };
    const sourceFilter = { field: 'source', values: [QuerySource.System] };
    const andFilters = getAndFilters(selectedColumnsFilter, { ...selectedUsersFilter, values: [] }, [
        entityFilter,
        sourceFilter,
    ]);
    const { data } = useAggregateAcrossEntitiesQuery({
        variables: {
            input: {
                facets: ['topUsersLast30DaysFeature'],
                query: '*',
                types: [EntityType.Query],
                orFilters: [{ and: andFilters }],
                searchFlags: {
                    maxAggValues: 100,
                },
            },
        },
        skip: !entityUrn,
    });

    const aggregations = data?.aggregateAcrossEntities?.facets?.find(
        (facet) => facet.field === 'topUsersLast30DaysFeature',
    )?.aggregations;

    const userAggregations =
        aggregations
            ?.map((agg) => ({
                value: agg.entity?.urn || agg.value,
                displayName: agg.entity?.urn ? entityRegistry.getDisplayName(agg.entity.type, agg.entity) : agg.value,
                count: agg.count,
            }))
            ?.sort((aggA, aggB) => aggB.count - aggA.count) || [];

    const usersFilter = { aggregations: userAggregations, displayName: 'Users', field: 'topUsersLast30DaysFeature' };

    return usersFilter;
}
