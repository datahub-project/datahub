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
        MISMATCHED_FIELD_TYPES_KEY_NAMES: 'Mismatched Type Fields',
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
export const ASSERTION_OPERATOR_TO_DESCRIPTION: Record<AssertionStdOperator, string | undefined> = {
    [AssertionStdOperator.EqualTo]: 'Is equal to',
    [AssertionStdOperator.NotEqualTo]: 'Is not equal to',
    [AssertionStdOperator.Contain]: 'Contains',
    [AssertionStdOperator.RegexMatch]: 'Matches',
    [AssertionStdOperator.StartWith]: 'Starts with',
    [AssertionStdOperator.EndWith]: 'Ends with',
    [AssertionStdOperator.In]: 'Is in',
    [AssertionStdOperator.NotIn]: 'Is not in',

    [AssertionStdOperator.IsFalse]: 'Is False',
    [AssertionStdOperator.IsTrue]: 'Is True',
    [AssertionStdOperator.Null]: 'Is NULL',
    [AssertionStdOperator.NotNull]: 'Is not NULL',

    [AssertionStdOperator.GreaterThan]: 'Is greater than',
    [AssertionStdOperator.GreaterThanOrEqualTo]: 'Is greater than or equal to',
    [AssertionStdOperator.LessThan]: 'Is less than',
    [AssertionStdOperator.LessThanOrEqualTo]: 'Is less than or equal to',
    [AssertionStdOperator.Between]: 'Is within a range',

    [AssertionStdOperator.Native]: undefined,
};
