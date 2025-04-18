import { useAggregateAcrossEntitiesQuery } from '../../../../../../graphql/search.generated';
import { ENTITY_FILTER_NAME, ORIGIN_FILTER_NAME } from '../../../../../search/utils/constants';
import { EntityType } from '../../../../../../types.generated';

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
