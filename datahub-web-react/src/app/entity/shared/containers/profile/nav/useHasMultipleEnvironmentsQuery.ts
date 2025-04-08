import { ENTITY_FILTER_NAME, ORIGIN_FILTER_NAME } from '@app/search/utils/constants';

import { useAggregateAcrossEntitiesQuery } from '@graphql/search.generated';
import { EntityType } from '@types';

export default function useHasMultipleEnvironmentsQuery(entityType: EntityType) {
    const { data } = useAggregateAcrossEntitiesQuery({
        variables: {
            input: {
                facets: [ORIGIN_FILTER_NAME],
                query: '*',
                orFilters: [{ and: [{ field: ENTITY_FILTER_NAME, values: [entityType] }] }],
            },
        },
    });
    const environmentAggs = data?.aggregateAcrossEntities?.facets?.find((facet) => facet.field === ORIGIN_FILTER_NAME);
    return environmentAggs && environmentAggs.aggregations.length > 1;
}
