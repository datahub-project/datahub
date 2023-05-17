import { useCallback } from 'react';
import { useAggregateAcrossEntitiesLazyQuery } from '../../../graphql/search.generated';
import { ORIGIN_FILTER_NAME } from '../utils/constants';
import { EntityType } from '../../../types.generated';

const useEnvironmentsQuery = () => {
    const [fetchAggregations, { data, loading, error }] = useAggregateAcrossEntitiesLazyQuery();

    const fetchEnvironments = useCallback(
        (countsForThisEntityType: EntityType) => {
            fetchAggregations({
                variables: {
                    input: {
                        types: [countsForThisEntityType],
                        query: '*',
                        orFilters: [],
                        viewUrn: null,
                        facets: [ORIGIN_FILTER_NAME],
                    },
                },
            });
        },
        [fetchAggregations],
    );

    const environments =
        data?.aggregateAcrossEntities?.facets
            ?.find((facet) => facet.field === ORIGIN_FILTER_NAME)
            ?.aggregations.filter((aggregation) => aggregation.count > 0) ?? [];

    return [fetchEnvironments, { loading, error, environments } as const] as const;
};

export default useEnvironmentsQuery;
