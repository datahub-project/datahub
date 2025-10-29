import { Maybe } from 'graphql/jsutils/Maybe';

import { ASSERTION_NATIVE_RESULTS_KEYS_BY_ASSERTION_TYPE } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/constants';
import { parseJsonArrayOrDefault, parseMaybeStringAsFloatOrDefault } from '@app/shared/numberUtil';

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
} from '@types';

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
function tryExtractNumericalValueFromNativeResults(
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
function tryExtractArrayValueFromNativeResults(
    nativeResults: Maybe<StringMapEntry[]> | undefined,
    key: string,
): any[] | undefined {
    const maybeValue = nativeResults?.find((result) => result.key === key)?.value;
    return parseJsonArrayOrDefault(maybeValue, undefined);
}

function tryExtractNumericalValueFromAssertionStdParameter(
    param?: Maybe<AssertionStdParameter>,
): number | undefined {
    let maybeNumber: undefined | number;
    if (param?.type === AssertionStdParameterType.Number) {
        maybeNumber = parseFloat(param.value);
        maybeNumber = !Number.isNaN(maybeNumber) ? maybeNumber : undefined;
    }
    return maybeNumber;
}

/**
 * Gets the main metric on an assertion's results that are being monitored over time
 * @param runEvent
 * @returns {number | undefined}
 */
export const tryGetPrimaryMetricValueFromAssertionRunEvent = (): number | undefined => {
    return undefined;
};

// This captures context around the expected range of values
type AssertionRangeEndType = 'inclusive' | 'exclusive';
type AssertionExpectedRange = {
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
