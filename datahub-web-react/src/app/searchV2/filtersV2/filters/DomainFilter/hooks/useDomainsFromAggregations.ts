/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useMemo } from 'react';

import { isDomain } from '@src/app/entityV2/domain/utils';
import { AggregationMetadata } from '@src/types.generated';

export default function useDomainsFromAggregations(aggregations: Array<AggregationMetadata> | undefined) {
    return useMemo(() => {
        const filteredAggregations = aggregations?.filter((aggregation) => aggregation.count > 0) ?? [];
        const entitiesFromAggregations = filteredAggregations.map((aggregation) => aggregation.entity);
        const domainsFromAggregations = entitiesFromAggregations.filter(isDomain);

        return domainsFromAggregations;
    }, [aggregations]);
}
