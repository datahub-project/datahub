import { AggregationMetadata } from '@src/types.generated';
import { useMemo } from 'react';
import { isDomain } from '@src/app/entityV2/domain/utils';

export default function useDomainsFromAggregations(aggregations: Array<AggregationMetadata> | undefined) {
    return useMemo(() => {
        const filteredAggregations = aggregations?.filter((aggregation) => aggregation.count > 0) ?? [];
        const entitiesFromAggregations = filteredAggregations.map((aggregation) => aggregation.entity);
        const domainsFromAggregations = entitiesFromAggregations.filter(isDomain);

        return domainsFromAggregations;
    }, [aggregations]);
}
