import { ASSERTION_TYPE_OPTIONS } from '@app/observe/dataset/assertion/constants';

import { AssertionType } from '@types';

/**
 * Returns the label for an assertion type.
 * @param assertionType - The assertion type.
 * @returns The label for the assertion type. If no label is found, returns the assertion type as a string.
 */
export const getAssertionTypeLabel = (assertionType: AssertionType): string => {
    const option = ASSERTION_TYPE_OPTIONS.find((opt) => opt.value === assertionType);
    return option?.name || String(assertionType);
};
