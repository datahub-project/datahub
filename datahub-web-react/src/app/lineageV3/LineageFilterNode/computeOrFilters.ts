import { TRANSFORMATION_PLATFORM_URNS } from '@app/lineageV3/transformationPlatforms';
import { ENTITY_FILTER_NAME, PLATFORM_FILTER_NAME } from '@app/searchV2/utils/constants';

import { AndFilterInput, EntityType, FacetFilterInput } from '@types';

/**
 * Returns or filters for getting related entities at depth 1, potentially filtering out transformations.
 * Transformations are defined as (type = dataset ^ platform is a transformation platform) v (type = datajob).
 * We can transform this into the correct format via logical equivalence:
 * (depth = 1) ^ ~((type = dataset ^ platform in T) v (type = datajob))
 * = (depth = 1) ^ ((type != dataset v platform not in T) ^ (type != datajob)) // De Morgan's Law
 * = (depth = 1 ^ type != dataset ^ type != datajob) v (depth = 1 ^ platform not in T ^ type != datajob) // Distributive Law
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
                { field: PLATFORM_FILTER_NAME, values: TRANSFORMATION_PLATFORM_URNS, negated: true },
            ],
        },
    ];
}
