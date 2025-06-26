import { useMemo } from 'react';

import { EntityTypeOption } from '@app/searchV2/filtersV2/filters/EntityTypeFilter/types';
import { getUniqueItemsByKeyFromArrrays } from '@app/searchV2/filtersV2/utils';
import { isEntityType } from '@src/app/entityV2/shared/utils';
import { FILTER_DELIMITER } from '@src/app/search/utils/constants';
import { FeildFacetState } from '@src/app/searchV2/filtersV2/types';
import { capitalizeFirstLetterOnly } from '@src/app/shared/textUtil';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';

export default function useOptions(facetState: FeildFacetState | undefined, values: string[]) {
    const entityRegistry = useEntityRegistryV2();

    const valuesFromAggregations = useMemo(
        () => facetState?.facet?.aggregations?.map((aggregation) => aggregation.value) ?? [],
        [facetState],
    );

    const uniqueValues = useMemo(
        () => getUniqueItemsByKeyFromArrrays([valuesFromAggregations, values]),
        [values, valuesFromAggregations],
    );

    const options: EntityTypeOption[] = useMemo(() => {
        return uniqueValues.map((value) => {
            const isSubtype = value.includes(FILTER_DELIMITER);
            if (isSubtype) {
                // Only one level in depth is possible
                const [parent, entitySubTypeName] = value.split(FILTER_DELIMITER);
                return {
                    value,
                    label: capitalizeFirstLetterOnly(entitySubTypeName) || '',
                    parentValue: parent,
                    displayName: entitySubTypeName,
                };
            }

            const hasChildren = uniqueValues.some((possibleChildrenValue) =>
                possibleChildrenValue.includes(value + FILTER_DELIMITER),
            );

            const displayName = (isEntityType(value) && entityRegistry.getEntityName(value)) || value;

            return {
                value,
                label: displayName,
                isParent: hasChildren,
                displayName,
            };
        });
    }, [uniqueValues, entityRegistry]);

    return options;
}
