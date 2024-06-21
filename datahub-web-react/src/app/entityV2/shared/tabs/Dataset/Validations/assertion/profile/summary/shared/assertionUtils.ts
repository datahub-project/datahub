import { Assertion, AssertionSourceType, AssertionType } from '../../../../../../../../../../types.generated';

export enum AssertionEditabilityScopeType {
    FULL = 'FULL',
    ACTIONS_AND_DESCRIPTION = 'ACTIONS_AND_DESCRIPTION',
    ACTIONS_ONLY = 'ACTIONS_ONLY',
    NONE = 'NONE',
}

// ------- NOTE: we take the lowest permission of the union of (AssertType, AssertionSourceType) from the maps below ------- //
const ASSERTION_TYPE_TO_EDITING_SCOPE: { [type in AssertionType]: AssertionEditabilityScopeType } = {
    [AssertionType.Custom]: AssertionEditabilityScopeType.FULL,
    [AssertionType.Field]: AssertionEditabilityScopeType.FULL,
    [AssertionType.Freshness]: AssertionEditabilityScopeType.FULL,
    [AssertionType.Sql]: AssertionEditabilityScopeType.FULL,
    [AssertionType.Volume]: AssertionEditabilityScopeType.FULL,
    [AssertionType.DataSchema]: AssertionEditabilityScopeType.FULL,
    [AssertionType.Dataset]: AssertionEditabilityScopeType.ACTIONS_AND_DESCRIPTION,
    [AssertionType.Custom]: AssertionEditabilityScopeType.ACTIONS_AND_DESCRIPTION,
};
const SOURCE_TYPE_TO_EDITING_SCOPE: { [type in AssertionSourceType]: AssertionEditabilityScopeType } = {
    [AssertionSourceType.Native]: AssertionEditabilityScopeType.FULL,
    [AssertionSourceType.External]: AssertionEditabilityScopeType.ACTIONS_AND_DESCRIPTION,
    [AssertionSourceType.Inferred]: AssertionEditabilityScopeType.ACTIONS_ONLY,
};

// Scope types that unlock the certain editing abilities (broadest to narrowest)
const TIERED_SCOPE_TYPES_MAP = {
    FULL: [AssertionEditabilityScopeType.FULL],
    ACTIONS_AND_DESCRIPTION: [
        AssertionEditabilityScopeType.ACTIONS_AND_DESCRIPTION,
        AssertionEditabilityScopeType.FULL,
    ],
    ACTIONS_ONLY: [
        AssertionEditabilityScopeType.ACTIONS_ONLY,
        AssertionEditabilityScopeType.ACTIONS_AND_DESCRIPTION,
        AssertionEditabilityScopeType.FULL,
    ],
};

/**
 * Gets the edabilityType of an asseriton
 * @param assertion
 * @returns {AssertionEditabilityScopeType}
 */
export const getAssertionEditabilityType = (assertion: Assertion): AssertionEditabilityScopeType => {
    const assertionType = assertion.info?.type;
    const assertionSourceType = assertion.info?.source?.type ?? AssertionSourceType.External; // NOTE: null==External since some of our integration connectors actually do not and historically have not produced this field
    if (!assertionType) {
        return AssertionEditabilityScopeType.NONE;
    }

    // 1. Full editing
    const isSourceTypeValidForFullEditing = TIERED_SCOPE_TYPES_MAP.FULL.includes(
        SOURCE_TYPE_TO_EDITING_SCOPE[assertionSourceType],
    );
    const isAssertionTypeValidForFullEditing = TIERED_SCOPE_TYPES_MAP.FULL.includes(
        ASSERTION_TYPE_TO_EDITING_SCOPE[assertionType],
    );
    if (isSourceTypeValidForFullEditing && isAssertionTypeValidForFullEditing) {
        return AssertionEditabilityScopeType.FULL;
    }

    // 2. Actions and description editing
    const isSourceTypeValidForActionAndDescriptionEditing = TIERED_SCOPE_TYPES_MAP.ACTIONS_AND_DESCRIPTION.includes(
        SOURCE_TYPE_TO_EDITING_SCOPE[assertionSourceType],
    );
    const isAssertionTypeValidForActionAndDescriptionEditing = TIERED_SCOPE_TYPES_MAP.ACTIONS_AND_DESCRIPTION.includes(
        ASSERTION_TYPE_TO_EDITING_SCOPE[assertionType],
    );
    if (isSourceTypeValidForActionAndDescriptionEditing && isAssertionTypeValidForActionAndDescriptionEditing) {
        return AssertionEditabilityScopeType.ACTIONS_AND_DESCRIPTION;
    }

    // 3. Actions only editing
    const isSourceTypeValidForActionOnlyEditing = TIERED_SCOPE_TYPES_MAP.ACTIONS_ONLY.includes(
        SOURCE_TYPE_TO_EDITING_SCOPE[assertionSourceType],
    );
    const isAssertionTypeValidForActionOnlyEditing = TIERED_SCOPE_TYPES_MAP.ACTIONS_ONLY.includes(
        ASSERTION_TYPE_TO_EDITING_SCOPE[assertionType],
    );
    if (isSourceTypeValidForActionOnlyEditing && isAssertionTypeValidForActionOnlyEditing) {
        return AssertionEditabilityScopeType.ACTIONS_ONLY;
    }

    return AssertionEditabilityScopeType.NONE;
};
