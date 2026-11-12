import { useCallback } from 'react';

import { PolicyMatchCondition, ResourceFilter } from '@types';

/**
 * Hook that handles condition changes and automatically clears values
 * when switching between StartsWith and other conditions.
 */
export const useClearOnConditionChange = (
    currentCondition: PolicyMatchCondition,
    fieldName: string,
    onConditionChange: (condition: PolicyMatchCondition, updatedResources: ResourceFilter) => void,
) => {
    return useCallback(
        (newCondition: PolicyMatchCondition, updatedResources: ResourceFilter) => {
            const wasStartsWith = currentCondition === PolicyMatchCondition.StartsWith;
            const isNowStartsWith = newCondition === PolicyMatchCondition.StartsWith;

            // Only clear if switching between StartsWith and other conditions
            if (wasStartsWith === isNowStartsWith) {
                onConditionChange(newCondition, updatedResources);
                return;
            }

            // Remove the field's criterion entirely rather than keeping it with empty
            // values: the UI renders an empty field as "applies to all", but the policy
            // engine requires every stored criterion to match and a criterion with no
            // values can never match — saving it would silently make the policy apply
            // to nothing. The chosen condition lives in the parent's conditions state
            // and is re-applied when values are next selected.
            const clearedResources = {
                ...updatedResources,
                filter: updatedResources.filter
                    ? {
                          ...updatedResources.filter,
                          criteria: (updatedResources.filter.criteria || []).filter((c) => c.field !== fieldName),
                      }
                    : updatedResources.filter,
            };

            onConditionChange(newCondition, clearedResources);
        },
        [currentCondition, fieldName, onConditionChange],
    );
};
