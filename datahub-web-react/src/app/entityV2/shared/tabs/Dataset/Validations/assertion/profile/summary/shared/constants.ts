import { AssertionStdOperator } from '../../../../../../../../../../types.generated';

// We hard code this here because there's no set schema NativeResults on the AssertionResult document
// - it's a generic map.
export const ASSERTION_NATIVE_RESULTS_KEYS_BY_ASSERTION_TYPE = {
    FIELD_ASSERTIONS: {
        FIELD_VALUES: {
            Y_VALUE_KEY_NAME: 'Invalid Rows',
            THRESHOLD_VALUE_KEY_NAME: 'Threshold Value', // NOTE: we're better off accessing this from the assertionInfo directly
        },
        METRIC_VALUES: {
            Y_VALUE_KEY_NAME: 'Metric Value',
            COMPARED_VALUE_KEY_NAME: 'Compared Value', // NOTE: we're better off accessing this from the assertionInfo directly
            COMPARED_MIN_VALUE_KEY_NAME: 'Compared Min Value', // ^
            COMPARED_MAX_VALUE_KEY_NAME: 'Compared Max Value', // ^
        },
    },
    SQL_ASSERTIONS: {
        Y_VALUE_KEY_NAME: 'Value',
        PREVIOUS_Y_VALUE_KEY_NAME: 'Previous Value',
    },
    VOLUME_ASSERTIONS: {
        PREVIOUS_Y_VALUE_KEY_NAME: 'Previous Row Count',
    },
    SCHEMA_ASSERTIONS: {
        EXTRA_FIELDS_IN_ACTUAL_KEY_NAME: 'Extra Fields in Actual',
        EXTRA_FIELDS_IN_EXPECTED_KEY_NAME: 'Extra Fields in Expected',
        MISMATCHED_TYPE_FIELDS_KEY_NAME: 'Mismatched Type Fields',
    },
    FRESHNESS_ASSERTIONS: {
        EVALUATION_WINDOW_START_TIME: 'Window Start Time',
        EVALUATION_WINDOW_END_TIME: 'Window End Time',
    },
};

export const ASSERTION_OPERATOR_DESCRIPTIONS_REQUIRING_SUFFIX = [
    AssertionStdOperator.EqualTo,
    AssertionStdOperator.NotEqualTo,
    AssertionStdOperator.Contain,
    AssertionStdOperator.RegexMatch,
    AssertionStdOperator.StartWith,
    AssertionStdOperator.EndWith,
    AssertionStdOperator.In,
    AssertionStdOperator.NotIn,
];

export const GET_ASSERTION_OPERATOR_TO_DESCRIPTION_MAP = ({ isPlural }) => ({
    [AssertionStdOperator.EqualTo]: `${isPlural ? 'are' : 'Is'} equal to`,
    [AssertionStdOperator.NotEqualTo]: `${isPlural ? 'are' : 'Is'} not equal to`,
    [AssertionStdOperator.Contain]: 'Contains',
    [AssertionStdOperator.RegexMatch]: 'Matches',
    [AssertionStdOperator.StartWith]: 'Starts with',
    [AssertionStdOperator.EndWith]: 'Ends with',
    [AssertionStdOperator.In]: `${isPlural ? 'are' : 'Is'} in`,
    [AssertionStdOperator.NotIn]: `${isPlural ? 'are' : 'Is'} not in`,
    [AssertionStdOperator.IsFalse]: `${isPlural ? 'are' : 'Is'} False`,
    [AssertionStdOperator.IsTrue]: `${isPlural ? 'are' : 'Is'} True`,
    [AssertionStdOperator.Null]: `${isPlural ? 'are' : 'Is'} NULL`,
    [AssertionStdOperator.NotNull]: `${isPlural ? 'are' : 'Is'} not NULL`,
    [AssertionStdOperator.GreaterThan]: `${isPlural ? 'are' : 'Is'} greater than`,
    [AssertionStdOperator.GreaterThanOrEqualTo]: `${isPlural ? 'are' : 'Is'} greater than or equal to`,
    [AssertionStdOperator.LessThan]: `${isPlural ? 'are' : 'Is'} less than`,
    [AssertionStdOperator.LessThanOrEqualTo]: `${isPlural ? 'are' : 'Is'} less than or equal to`,
    [AssertionStdOperator.Between]: `${isPlural ? 'are' : 'Is'} within a range`,
    [AssertionStdOperator.Native]: undefined,
});
