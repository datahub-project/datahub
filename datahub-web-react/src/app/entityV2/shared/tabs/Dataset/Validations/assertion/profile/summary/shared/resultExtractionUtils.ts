import { Maybe } from 'graphql/jsutils/Maybe';
import {
    AssertionResult,
    AssertionResultType,
    AssertionStdOperator,
    AssertionStdParameter,
    AssertionStdParameterType,
    AssertionValueChangeType,
    FieldMetricAssertion,
    FieldValuesAssertion,
    FieldValuesFailThresholdType,
    IncrementingSegmentRowCountChange,
    IncrementingSegmentRowCountTotal,
    RowCountChange,
    RowCountTotal,
    SqlAssertionInfo,
    StringMapEntry,
} from '../../../../../../../../../../types.generated';
import { ASSERTION_NATIVE_RESULTS_KEYS_BY_ASSERTION_TYPE } from './constants';
import {
    parseJsonArrayOrDefault,
    parseMaybeStringAsFloatOrDefault,
} from '../../../../../../../../../shared/numberUtil';

/**
 * Calculates expected value of an assertion given previous value and change modifier details
 * ie. { previous row count = 1000, changeType=%, modifier=100 } | expectedOutput=2000
 * @param previousValue
 * @param changeType
 * @param expectedChangeModifier
 * @returns {number | undefined}
 */
const calculateExpectedNumericalValueWithPreviousNumericalValue = (
    previousValue: number,
    changeType: AssertionValueChangeType,
    expectedChangeModifier: number,
): number | undefined => {
    let expectedRowCount: undefined | number;
    switch (changeType) {
        case AssertionValueChangeType.Absolute:
            expectedRowCount = previousValue + expectedChangeModifier;
            break;
        case AssertionValueChangeType.Percentage:
            expectedRowCount = previousValue * (1 + expectedChangeModifier / 100);
            break;
        default:
            break;
    }
    return expectedRowCount;
};

/**
 * Attempts to extract a numerical value from native results with a given key
 * @param nativeResults
 * @param key
 * @returns {number | undefined}
 */
export function tryExtractNumericalValueFromNativeResults(
    nativeResults: Maybe<StringMapEntry[]> | undefined,
    key: string,
): number | undefined {
    const maybeValue = nativeResults?.find((result) => result.key === key)?.value;
    return parseMaybeStringAsFloatOrDefault(maybeValue, undefined);
}

/**
 * Attempts to extract a JSON array value from native results with a given key
 * @param nativeResults
 * @param key
 * @returns {number | undefined}
 */
export function tryExtractArrayValueFromNativeResults(
    nativeResults: Maybe<StringMapEntry[]> | undefined,
    key: string,
): any[] | undefined {
    const maybeValue = nativeResults?.find((result) => result.key === key)?.value;
    return parseJsonArrayOrDefault(maybeValue, undefined);
}

export function tryExtractNumericalValueFromAssertionStdParameter(
    param?: Maybe<AssertionStdParameter>,
): number | undefined {
    let maybeNumber: undefined | number;
    if (param?.type === AssertionStdParameterType.Number) {
        maybeNumber = parseFloat(param.value);
        maybeNumber = !Number.isNaN(maybeNumber) ? maybeNumber : undefined;
    }
    return maybeNumber;
}

export const tryGetFieldMetricAssertionNumericalResult = (result?: Maybe<AssertionResult>): number | undefined => {
    return tryExtractNumericalValueFromNativeResults(
        result?.nativeResults,
        ASSERTION_NATIVE_RESULTS_KEYS_BY_ASSERTION_TYPE.FIELD_ASSERTIONS.METRIC_VALUES.Y_VALUE_KEY_NAME,
    );
};
export const tryGetFieldValueAssertionNumericalResult = (result?: Maybe<AssertionResult>) => {
    if (result?.type === AssertionResultType.Init) return 0;
    return tryExtractNumericalValueFromNativeResults(
        result?.nativeResults,
        ASSERTION_NATIVE_RESULTS_KEYS_BY_ASSERTION_TYPE.FIELD_ASSERTIONS.FIELD_VALUES.Y_VALUE_KEY_NAME,
    );
};

export const tryGetSqlAssertionNumericalResult = (result?: Maybe<AssertionResult>) => {
    return tryExtractNumericalValueFromNativeResults(
        result?.nativeResults,
        ASSERTION_NATIVE_RESULTS_KEYS_BY_ASSERTION_TYPE.SQL_ASSERTIONS.Y_VALUE_KEY_NAME,
    );
};

export const tryGetPreviousSqlAssertionNumericalResult = (result?: Maybe<AssertionResult>) => {
    return tryExtractNumericalValueFromNativeResults(
        result?.nativeResults,
        ASSERTION_NATIVE_RESULTS_KEYS_BY_ASSERTION_TYPE.SQL_ASSERTIONS.PREVIOUS_Y_VALUE_KEY_NAME,
    );
};

export const tryGetAbsoluteVolumeAssertionNumericalResult = (result?: Maybe<AssertionResult>) => {
    const rowCount = result?.rowCount;
    return rowCount || undefined;
};

export const tryGetPreviousVolumeAssertionNumericalResult = (result?: Maybe<AssertionResult>) => {
    return tryExtractNumericalValueFromNativeResults(
        result?.nativeResults,
        ASSERTION_NATIVE_RESULTS_KEYS_BY_ASSERTION_TYPE.VOLUME_ASSERTIONS.PREVIOUS_Y_VALUE_KEY_NAME,
    );
};
export const tryGetActualUpdatedTimestampFromAssertionResult = (result?: Maybe<AssertionResult>) => {
    const eventsArrString = result?.nativeResults?.find((pair) => pair.key === 'events')?.value;
    const eventsArr = eventsArrString ? JSON.parse(eventsArrString) : [];
    const timestamps = eventsArr.map((event) => event.time);
    const sortedTimestampsDescending = timestamps.sort((a, b) => b - a);
    const maxTimestamp = sortedTimestampsDescending[0];
    return maxTimestamp;
};

export const tryGetExtraFieldsInActual = (result?: AssertionResult | null) => {
    return tryExtractArrayValueFromNativeResults(
        result?.nativeResults,
        ASSERTION_NATIVE_RESULTS_KEYS_BY_ASSERTION_TYPE.SCHEMA_ASSERTIONS.EXTRA_FIELDS_IN_ACTUAL_KEY_NAME,
    );
};

export const tryGetExtraFieldsInExpected = (result?: AssertionResult | null) => {
    return tryExtractArrayValueFromNativeResults(
        result?.nativeResults,
        ASSERTION_NATIVE_RESULTS_KEYS_BY_ASSERTION_TYPE.SCHEMA_ASSERTIONS.EXTRA_FIELDS_IN_EXPECTED_KEY_NAME,
    );
};

export const tryGetMismatchedTypeFields = (result?: AssertionResult | null) => {
    return tryExtractArrayValueFromNativeResults(
        result?.nativeResults,
        ASSERTION_NATIVE_RESULTS_KEYS_BY_ASSERTION_TYPE.SCHEMA_ASSERTIONS.MISMATCHED_TYPE_FIELDS_KEY_NAME,
    );
};

/**
 * Gets the main metric on an assertion's results that are being monitored over time
 * @param runEvent
 * @returns {number | undefined}
 */
export const tryGetPrimaryMetricValueFromAssertionRunEvent = (): number | undefined => {
    return undefined;
};

// This captures context around the expected range of values
export type AssertionRangeEndType = 'inclusive' | 'exclusive';
export type AssertionExpectedRange = {
    // These are the actual values we expect (ie. actual row count)
    high?: number;
    low?: number;
    // This contains extra context about this range
    context?: {
        highType?: AssertionRangeEndType;
        lowType?: AssertionRangeEndType;
        // This contains context for relative assertions (ie. grows by max 10%, min 5%)
        relativeModifiers?: {
            type: AssertionValueChangeType; // percent vs absolute
            high?: number;
            low?: number;
        };
    };
};

/**
 * Tries to calculate expected result ranges for assertions that have fixed numerical expectations
 * ie. handles 'RowCount should be between 10k and 50k'
 * @param totals
 * @returns
 */
export function tryGetExpectedRangeFromAssertionAgainstAbsoluteValues(
    totals?:
        | Maybe<IncrementingSegmentRowCountTotal>
        | Maybe<RowCountTotal>
        | Maybe<SqlAssertionInfo>
        | Maybe<FieldValuesAssertion>
        | Maybe<FieldMetricAssertion>,
): AssertionExpectedRange {
    if (!totals?.parameters) {
        return {};
    }
    let high: undefined | number;
    let low: undefined | number;
    let highType: undefined | AssertionRangeEndType;
    let lowType: undefined | AssertionRangeEndType;
    switch (totals?.operator) {
        case AssertionStdOperator.Between:
            high = tryExtractNumericalValueFromAssertionStdParameter(totals.parameters.maxValue);
            low = tryExtractNumericalValueFromAssertionStdParameter(totals.parameters.minValue);
            highType = 'inclusive';
            lowType = 'inclusive';
            break;
        case AssertionStdOperator.GreaterThan:
            low = tryExtractNumericalValueFromAssertionStdParameter(totals.parameters.value);
            lowType = 'exclusive';
            break;
        case AssertionStdOperator.GreaterThanOrEqualTo:
            low = tryExtractNumericalValueFromAssertionStdParameter(totals.parameters.value);
            lowType = 'inclusive';
            break;
        case AssertionStdOperator.LessThan:
            high = tryExtractNumericalValueFromAssertionStdParameter(totals.parameters.value);
            highType = 'exclusive';
            break;
        case AssertionStdOperator.LessThanOrEqualTo:
            high = tryExtractNumericalValueFromAssertionStdParameter(totals.parameters.value);
            highType = 'inclusive';
            break;
        default:
            break;
    }
    return {
        high,
        low,
        context: {
            highType,
            lowType,
        },
    };
}
/**
 * Tries to calculate expected result ranges for assertions that have expectations defined relative to previous runs
 * ie. handles 'RowCount should not grow by more than 50%'
 * @param changingInfo
 * @param changeType
 * @param previousCount
 * @returns {AssertionExpectedRange}
 */
export function tryGetExpectedRangeFromAssertionAgainstRelativeValues(
    changingInfo?: Maybe<RowCountChange | IncrementingSegmentRowCountChange | SqlAssertionInfo>,
    changeType?: Maybe<AssertionValueChangeType>,
    previousCount?: Maybe<number>,
): AssertionExpectedRange {
    if (
        !changingInfo?.parameters ||
        typeof previousCount !== 'number' ||
        typeof changeType === 'undefined' ||
        changeType === null
    ) {
        return {};
    }

    let high: undefined | number;
    let low: undefined | number;
    let highType: undefined | AssertionRangeEndType;
    let lowType: undefined | AssertionRangeEndType;
    let modifierHigh: undefined | number;
    let modifierLow: undefined | number;

    switch (changingInfo?.operator) {
        case AssertionStdOperator.Between: {
            modifierHigh = tryExtractNumericalValueFromAssertionStdParameter(changingInfo.parameters.maxValue);
            modifierLow = tryExtractNumericalValueFromAssertionStdParameter(changingInfo.parameters.minValue);
            high =
                typeof modifierHigh === 'number'
                    ? calculateExpectedNumericalValueWithPreviousNumericalValue(previousCount, changeType, modifierHigh)
                    : undefined;
            low =
                typeof modifierLow === 'number'
                    ? calculateExpectedNumericalValueWithPreviousNumericalValue(previousCount, changeType, modifierLow)
                    : undefined;

            highType = 'inclusive';
            lowType = 'inclusive';
            break;
        }
        case AssertionStdOperator.GreaterThan: {
            modifierLow = tryExtractNumericalValueFromAssertionStdParameter(changingInfo.parameters.value);
            low =
                typeof modifierLow === 'number'
                    ? calculateExpectedNumericalValueWithPreviousNumericalValue(previousCount, changeType, modifierLow)
                    : undefined;
            lowType = 'exclusive';
            break;
        }
        case AssertionStdOperator.GreaterThanOrEqualTo: {
            modifierLow = tryExtractNumericalValueFromAssertionStdParameter(changingInfo.parameters.value);
            low =
                typeof modifierLow === 'number'
                    ? calculateExpectedNumericalValueWithPreviousNumericalValue(previousCount, changeType, modifierLow)
                    : undefined;
            lowType = 'inclusive';
            break;
        }
        case AssertionStdOperator.LessThan: {
            modifierHigh = tryExtractNumericalValueFromAssertionStdParameter(changingInfo.parameters.value);
            high =
                typeof modifierHigh === 'number'
                    ? calculateExpectedNumericalValueWithPreviousNumericalValue(previousCount, changeType, modifierHigh)
                    : undefined;
            highType = 'exclusive';
            break;
        }
        case AssertionStdOperator.LessThanOrEqualTo: {
            modifierHigh = tryExtractNumericalValueFromAssertionStdParameter(changingInfo.parameters.value);
            high =
                typeof modifierHigh === 'number'
                    ? calculateExpectedNumericalValueWithPreviousNumericalValue(previousCount, changeType, modifierHigh)
                    : undefined;
            highType = 'inclusive';
            break;
        }
        default:
            break;
    }
    return {
        high,
        low,
        context: {
            highType,
            lowType,
            relativeModifiers: {
                type: changeType,
                high: modifierHigh,
                low: modifierLow,
            },
        },
    };
}

export function tryGetExpectedRangeFromFailThreshold(
    fieldValuesAssertion?: Maybe<FieldValuesAssertion>,
): AssertionExpectedRange {
    const highType: AssertionRangeEndType = 'inclusive';
    let high: number | undefined;

    const thresholdType = fieldValuesAssertion?.failThreshold?.type;
    switch (thresholdType) {
        case FieldValuesFailThresholdType.Count:
            high = parseMaybeStringAsFloatOrDefault(fieldValuesAssertion?.failThreshold.value);
            break;
        case FieldValuesFailThresholdType.Percentage:
            break;
        default:
            break;
    }

    return {
        high,
        context: {
            highType,
        },
    };
}
