import { Assertion, AssertionSourceType, AssertionType } from '../../../../../../../../../../types.generated';

const FULLY_EDITABLE_ASSERTION_TYPES = new Set([
    AssertionType.Field,
    AssertionType.Freshness,
    AssertionType.Sql,
    AssertionType.Volume,
]);
const ASSERTION_ACTION_EDITING_ONLY_ASSERTION_TYPES = new Set([
    AssertionType.Dataset,
])

// Users cannot edit parameters external assetions or inferred system assertions directly. Only the assertion actions (ie incident raising).
const ASSERTION_ACTION_EDITING_ONLY_SOURCE_TYPES = new Set<AssertionSourceType>([AssertionSourceType.Inferred, AssertionSourceType.External]);
export const getAssertionEditabilityType = (assertion: Assertion): 'full' | 'actions-only' | 'none' => {
    const assertionType = assertion.info?.type;
    const assertionSourceType = assertion.info?.source?.type;
    if (!assertionType || !assertionSourceType) {
        return 'none'
    }

    const isSourceTypeValidForFullEditing = !ASSERTION_ACTION_EDITING_ONLY_SOURCE_TYPES.has(assertionSourceType)
    const isAssertionTypeValidForFullEditing = FULLY_EDITABLE_ASSERTION_TYPES.has(assertionType)

    if (isAssertionTypeValidForFullEditing && isSourceTypeValidForFullEditing) {
        return 'full'
    }

    const isSourceTypeValidForActionEditing = ASSERTION_ACTION_EDITING_ONLY_SOURCE_TYPES.has(assertionSourceType);
    const isAssertionTypeValidForActionEditing = ASSERTION_ACTION_EDITING_ONLY_ASSERTION_TYPES.has(assertionType);

    if (isSourceTypeValidForActionEditing || isAssertionTypeValidForActionEditing) {
        return 'actions-only';
    }

    return 'none';
};