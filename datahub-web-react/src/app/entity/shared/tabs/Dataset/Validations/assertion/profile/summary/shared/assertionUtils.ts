import { Assertion, AssertionSourceType, AssertionType } from '../../../../../../../../../../types.generated';

const EDITABLE_ASSERTION_TYPES = new Set([
    AssertionType.Field,
    AssertionType.Freshness,
    AssertionType.Sql,
    AssertionType.Volume,
]);

// Users cannot edit external assetions or inferred system assertions directly.
const UNSUPPORTED_SOURCE_TYPES = new Set([AssertionSourceType.External, AssertionSourceType.Inferred]);

export const isEditableAssertion = (assertion: Assertion) => {
    const assertionType = assertion.info?.type;
    const assertionSourceType = assertion.info?.source?.type;

    return (
        assertionType &&
        assertionSourceType &&
        EDITABLE_ASSERTION_TYPES.has(assertionType) &&
        !UNSUPPORTED_SOURCE_TYPES.has(assertionSourceType)
    );
};
