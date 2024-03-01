import { AssertionType } from "../../../../../../../../../../types.generated";

export const VALUES_OVER_TIME_ASSERTION_TYPES: AssertionType[] = [AssertionType.Field, AssertionType.Sql, AssertionType.Volume];

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
        }
    },
    SQL_ASSERTIONS: {
        Y_VALUE_KEY_NAME: 'Value',
        PREVIOUS_Y_VALUE_KEY_NAME: 'Previous Value',
    },
}