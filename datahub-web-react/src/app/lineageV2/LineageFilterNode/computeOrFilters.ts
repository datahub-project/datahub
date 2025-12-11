/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { DBT_URN } from '@app/ingest/source/builder/constants';
import { ENTITY_FILTER_NAME, PLATFORM_FILTER_NAME } from '@app/searchV2/utils/constants';

import { AndFilterInput, EntityType, FacetFilterInput } from '@types';

/**
 * Returns or filters for getting related entities at depth 1, potentially filtering out transformations.
 * Transformations are defined as (type = dataset ^ platform = dbt) v (type = datajob).
 * We can transform this into the correct format via logical equivalence:
 * (depth = 1) ^ ~((type = dataset ^ platform = dbt) v (type = datajob))
 * = (depth = 1) ^ ((type != dataset v platform != dbt) ^ (type != datajob)) // De Morgan's Law
 * = (depth = 1 ^ type != dataset ^ type != datajob) v (depth = 1 ^ platform != dbt ^ type != datajob) // Distributive Law
 */
export default function computeOrFilters(
    defaultFilters: FacetFilterInput[],
    hideTransformations = true,
    hideDataProcessInstances = true,
): AndFilterInput[] {
    if (!hideTransformations && !hideDataProcessInstances) {
        return [{ and: defaultFilters }];
    }

    if (!hideTransformations) {
        return [
            {
                and: [
                    ...defaultFilters,
                    {
                        field: ENTITY_FILTER_NAME,
                        values: [EntityType.DataProcessInstance],
                        negated: true,
                    },
                ],
            },
        ];
    }

    return [
        {
            and: [
                ...defaultFilters,
                {
                    field: ENTITY_FILTER_NAME,
                    values: [EntityType.Dataset, EntityType.DataJob],
                    negated: true,
                },
            ],
        },
        {
            and: [
                ...defaultFilters,
                {
                    field: ENTITY_FILTER_NAME,
                    values: [EntityType.DataJob],
                    negated: true,
                },
                { field: PLATFORM_FILTER_NAME, values: [DBT_URN], negated: true },
            ],
        },
    ];
}
