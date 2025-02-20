import { AssertionSourceType, AssertionType } from '../../../../../../../../../types.generated';

/**
 * Returns true if the assertion is definitely an external assertion,
 * meaning that was reported using a custom source or a well-supported integration
 * like dbt test or Great Expectations.
 */
export const isExternalAssertion = (assertion) => {
    return (
        assertion?.info?.type === AssertionType.Dataset || // Legacy, non-native assertion type.
        assertion?.info?.type === AssertionType.Custom ||
        (assertion?.info?.source?.type !== AssertionSourceType.Inferred &&
            assertion?.info?.source?.type !== AssertionSourceType.Native)
    );
};
