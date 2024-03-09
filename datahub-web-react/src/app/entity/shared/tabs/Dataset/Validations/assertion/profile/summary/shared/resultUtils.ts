import { Maybe } from 'graphql/jsutils/Maybe';
import {
    Assertion,
    AssertionResult,
    AssertionResultType,
    AssertionRunEvent,
    AssertionStdOperator,
    AssertionStdParameter,
    AssertionStdParameterType,
    AssertionType,
    AssertionValueChangeType,
    FieldAssertionInfo,
    FieldAssertionType,
    FieldMetricAssertion,
    FieldValuesAssertion,
    IncrementingSegmentRowCountChange,
    IncrementingSegmentRowCountTotal,
    RowCountChange,
    RowCountTotal,
    SqlAssertionInfo,
    SqlAssertionType,
    StringMapEntry,
    VolumeAssertionInfo,
    VolumeAssertionType,
} from '../../../../../../../../../../types.generated';
import { formatNumberWithoutAbbreviation } from '../../../../../../../../../shared/formatNumber';
import { toLocalDateString, toLocalTimeString } from '../../../../../../../../../shared/time/timeUtils';
import { getResultErrorMessage } from '../../../../assertionUtils';
import { getFieldMetricLabel } from '../../../builder/steps/field/utils';
import { ASSERTION_NATIVE_RESULTS_KEYS_BY_ASSERTION_TYPE } from './constants';

export const getFormattedResultText = (result?: AssertionResultType) => {
    if (result === undefined) {
        return 'Not completed';
    }
    if (result === AssertionResultType.Success) {
        return 'Passing';
    }
    if (result === AssertionResultType.Failure) {
        return 'Failing';
    }
    if (result === AssertionResultType.Error) {
        return 'Error';
    }
    if (result === AssertionResultType.Init) {
        return 'Initializing';
    }
    return undefined;
};

const calculateMetricChangePercentage = (previous: number, actual: number) => {
    return previous ? ((actual - previous) / previous) * 100 : undefined;
};

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
            expectedRowCount = previousValue + expectedChangeModifier
            break;
        case AssertionValueChangeType.Percentage:
            expectedRowCount = previousValue * (1 + (expectedChangeModifier / 100))
            break;
        default:
            break;
    }
    return expectedRowCount
}

/**
 * Attempts to extract a numerical value from native results with a given key
 * @param nativeResults 
 * @param key 
 * @returns {number | undefined}
 */
export function tryExtractNumericalValueFromNativeResults(nativeResults: Maybe<StringMapEntry[]> | undefined, key: string): number | undefined {
    const maybeValue = nativeResults?.find(result => result.key === key)?.value
    const parsedValue = typeof maybeValue === 'string' ? parseFloat(maybeValue) : maybeValue
    return typeof parsedValue === 'number' && !Number.isNaN(parsedValue) ? parsedValue : undefined;
}

export function tryExtractNumericalValueFromAssertionStdParameter(param?: Maybe<AssertionStdParameter>): number | undefined {
    let maybeNumber: undefined | number
    if (param?.type === AssertionStdParameterType.Number) {
        maybeNumber = parseFloat(param.value)
        maybeNumber = !Number.isNaN(maybeNumber) ? maybeNumber : undefined;
    }
    return maybeNumber;
}

const tryGetFieldMetricAssertionNumericalResult = (result?: Maybe<AssertionResult>): number | undefined => {
    return tryExtractNumericalValueFromNativeResults(result?.nativeResults, ASSERTION_NATIVE_RESULTS_KEYS_BY_ASSERTION_TYPE.FIELD_ASSERTIONS.METRIC_VALUES.Y_VALUE_KEY_NAME);
};
const tryGetFieldValueAssertionNumericalResult = (result?: Maybe<AssertionResult>) => {
    if (result?.type === AssertionResultType.Init) return 0;
    return tryExtractNumericalValueFromNativeResults(result?.nativeResults, ASSERTION_NATIVE_RESULTS_KEYS_BY_ASSERTION_TYPE.FIELD_ASSERTIONS.FIELD_VALUES.Y_VALUE_KEY_NAME);
};

const tryGetSqlAssertionNumericalResult = (result?: Maybe<AssertionResult>) => {
    return tryExtractNumericalValueFromNativeResults(result?.nativeResults, ASSERTION_NATIVE_RESULTS_KEYS_BY_ASSERTION_TYPE.SQL_ASSERTIONS.Y_VALUE_KEY_NAME);
};

const tryGetPreviousSqlAssertionNumericalResult = (result?: Maybe<AssertionResult>) => {
    return tryExtractNumericalValueFromNativeResults(result?.nativeResults, ASSERTION_NATIVE_RESULTS_KEYS_BY_ASSERTION_TYPE.SQL_ASSERTIONS.PREVIOUS_Y_VALUE_KEY_NAME);
};

const tryGetAbsoluteVolumeAssertionNumericalResult = (result?: Maybe<AssertionResult>) => {
    const rowCount = result?.rowCount;
    return rowCount || undefined;
};

const tryGetPreviousVolumeAssertionNumericalResult = (result?: Maybe<AssertionResult>) => {
    return tryExtractNumericalValueFromNativeResults(result?.nativeResults, ASSERTION_NATIVE_RESULTS_KEYS_BY_ASSERTION_TYPE.VOLUME_ASSERTIONS.PREVIOUS_Y_VALUE_KEY_NAME);
};
const tryGetActualUpdatedTimestampFromAssertionResult = (result?: Maybe<AssertionResult>) => {
    const eventsArrString = result?.nativeResults?.find((pair) => pair.key === 'events')?.value;
    const eventsArr = eventsArrString ? JSON.parse(eventsArrString) : [];
    const timestamps = eventsArr.map((event) => event.time);
    const sortedTimestampsDescending = timestamps.sort((a, b) => b - a);
    const maxTimestamp = sortedTimestampsDescending[0];
    return maxTimestamp;
};

// TODO: Consider supporting relative field metric assertions.
const getFormattedReasonTextForFieldMetricAssertion = (assertion: Assertion, run: AssertionRunEvent) => {
    const field = assertion.info?.fieldAssertion?.fieldMetricAssertion?.field?.path || 'column';
    const metricType = assertion.info?.fieldAssertion?.fieldMetricAssertion?.metric;
    const metricText = (metricType && getFieldMetricLabel(metricType)) || 'Aggregation';
    const actual = tryGetFieldMetricAssertionNumericalResult(run.result);
    const result = run.result?.type;
    if (result === AssertionResultType.Success) {
        if (actual !== undefined) {
            return `${metricText} of ${field} (${actual}) met the expected conditions.`;
        }
        return `${metricText} of ${field} met the expected conditions.`;
    }
    if (actual !== undefined) {
        return `${metricText} of ${field} (${actual}) did not meet the expected conditions.`;
    }
    return `${metricText} of ${field} did not meet the expected conditions.`;
};

const getFormattedReasonTextForFieldValuesAssertion = (assertion: Assertion, run: AssertionRunEvent) => {
    const field = assertion.info?.fieldAssertion?.fieldValuesAssertion?.field?.path || 'column';
    const invalidRowsCount = tryGetFieldValueAssertionNumericalResult(run.result);
    const result = run.result?.type;
    if (result === AssertionResultType.Success) {
        return `All rows values met the expected conditions for column ${field}.`;
    }
    if (invalidRowsCount !== undefined) {
        return `${invalidRowsCount} rows did not meet the expected conditions for column ${field}.`;
    }
    return `Some rows did not meet the expected conditions for column ${field}.`;
};

const getFormattedReasonTextForFieldAssertion = (assertion: Assertion, run: AssertionRunEvent) => {
    const field = assertion.info?.fieldAssertion;
    if (field?.type === FieldAssertionType.FieldMetric) {
        return getFormattedReasonTextForFieldMetricAssertion(assertion, run);
    }
    return getFormattedReasonTextForFieldValuesAssertion(assertion, run);
};

const getFormattedReasonTextForAbsoluteSqlAssertion = (_: Assertion, run: AssertionRunEvent) => {
    // Be careful about showing the actual result that was returned, since it may contain sensitive information.
    const result = run.result?.type;
    if (result === AssertionResultType.Success) {
        return `The result of the provided SQL query met the expected conditions.`;
    }
    return `The result of the provided SQL query did not meet the expected conditions.`;
};

const getFormattedReasonTextForRelativeSqlAssertion = (_: Assertion, run: AssertionRunEvent) => {
    const result = run.result?.type;
    const actualRowCount = tryGetSqlAssertionNumericalResult(run.result);
    const previousRowCount = tryGetPreviousSqlAssertionNumericalResult(run.result);
    const rowCountChangePercentage =
        actualRowCount && previousRowCount && calculateMetricChangePercentage(previousRowCount, actualRowCount);
    const rowCountChangeTotal = actualRowCount && previousRowCount ? actualRowCount - previousRowCount : 0;
    const positiveChange = (rowCountChangeTotal || 0) >= 0;
    const rowCountChangeText = `${formatNumberWithoutAbbreviation(rowCountChangeTotal)} (${positiveChange ? '+' : '-'
        }${rowCountChangePercentage}%)`;

    if (result === AssertionResultType.Success) {
        if (actualRowCount !== undefined && previousRowCount !== undefined) {
            return `The change in the SQL query result of ${rowCountChangeText} met the expected conditions.`;
        }
        return `The change in the SQL query result met the expected conditions.`;
    }
    if (actualRowCount !== undefined && previousRowCount !== undefined) {
        return `The change in the SQL query result of ${rowCountChangeText} did not meet the expected conditions.`;
    }
    return `The change in the SQL query result did not meet the expected conditions.`;
};

const getFormattedReasonTextForSqlAssertion = (assertion: Assertion, run: AssertionRunEvent) => {
    const isAbsolute = assertion?.info?.sqlAssertion?.type === SqlAssertionType.Metric;
    return isAbsolute
        ? getFormattedReasonTextForAbsoluteSqlAssertion(assertion, run)
        : getFormattedReasonTextForRelativeSqlAssertion(assertion, run);
};

const getFormattedReasonTextForFreshnessAssertion = (_: Assertion, run: AssertionRunEvent) => {
    // Be careful about showing the actual result that was returned, since it may contain sensitive information.
    const result = run.result?.type;
    const actualTimestamp = tryGetActualUpdatedTimestampFromAssertionResult(run.result);
    const formattedTime = (actualTimestamp && toLocalTimeString(actualTimestamp)) || undefined;
    const formattedDate = (actualTimestamp && toLocalDateString(actualTimestamp)) || undefined;
    if (result === AssertionResultType.Success) {
        if (formattedTime) {
            return `Table was updated within the expected timeframe at ${formattedTime} on ${formattedDate}.`;
        }
        return `The table was updated within the expected timeframe.`;
    }
    return `No table updates occurred within the expected timeframe.`;
};

const getFormattedReasonTextForAbsoluteVolumeAssertion = (_: Assertion, run: AssertionRunEvent) => {
    const result = run.result?.type;
    const actualRowCount = tryGetAbsoluteVolumeAssertionNumericalResult(run.result);
    if (result === AssertionResultType.Success) {
        if (actualRowCount !== undefined) {
            return `The actual row count (${formatNumberWithoutAbbreviation(
                actualRowCount,
            )}) met the expected conditions.`;
        }
        return `The actual row count met the expected conditions.`;
    }
    if (actualRowCount !== undefined) {
        return `The actual row count (${formatNumberWithoutAbbreviation(
            actualRowCount,
        )}) did not meet the expected conditions.`;
    }
    return `The actual row count did not meet the expected conditions.`;
};

const getFormattedReasonTextForRelativeVolumeAssertion = (_: Assertion, run: AssertionRunEvent) => {
    const result = run.result?.type;
    const actualRowCount = tryGetAbsoluteVolumeAssertionNumericalResult(run.result);
    const previousRowCount = tryGetPreviousVolumeAssertionNumericalResult(run.result);
    const rowCountChangePercentage =
        actualRowCount && previousRowCount && calculateMetricChangePercentage(previousRowCount, actualRowCount);
    const rowCountChangeTotal = actualRowCount && previousRowCount ? actualRowCount - previousRowCount : 0;
    const positiveChange = (rowCountChangeTotal || 0) >= 0;
    const rowCountChangeText = `${formatNumberWithoutAbbreviation(rowCountChangeTotal)} (${positiveChange ? '+' : '-'
        }${rowCountChangePercentage}%)`;

    if (result === AssertionResultType.Success) {
        if (actualRowCount !== undefined && previousRowCount !== undefined) {
            return `The change in row count of ${rowCountChangeText} met the expected conditions.`;
        }
        return `The change in row count met the expected conditions.`;
    }
    if (actualRowCount !== undefined && previousRowCount !== undefined) {
        return `The change in row count of ${rowCountChangeText} did not meet the expected conditions.`;
    }
    return `The change in row count did not meet the expected conditions.`;
};

const getFormattedReasonTextForVolumeAssertion = (assertion: Assertion, run: AssertionRunEvent) => {
    // TODO: Since we are allowing edits, this will need to be baked into the run itself.
    const isAbsolute = !!assertion?.info?.volumeAssertion?.rowCountTotal;
    return isAbsolute
        ? getFormattedReasonTextForAbsoluteVolumeAssertion(assertion, run)
        : getFormattedReasonTextForRelativeVolumeAssertion(assertion, run);
};

const getFormattedReasonTextForDefaultAssertion = (_: Assertion, run: AssertionRunEvent) => {
    const result = run.result?.type;
    if (result === AssertionResultType.Success) {
        return `The expected conditions were met`;
    }
    return `The expected conditions were not met.`;
};

export const getFormattedReasonText = (assertion: Assertion, run: AssertionRunEvent) => {
    if (run?.result?.type === AssertionResultType.Init) {
        return 'Collecting information required to evaluate conditions. Results will be available on the next evaluation.';
    }
    if (run?.result?.type === AssertionResultType.Error) {
        const formattedError = getResultErrorMessage(run?.result);
        return `${formattedError}`;
    }

    switch (assertion.info?.type) {
        case AssertionType.Freshness:
            return getFormattedReasonTextForFreshnessAssertion(assertion, run);
        case AssertionType.Volume:
            return getFormattedReasonTextForVolumeAssertion(assertion, run);
        case AssertionType.Field:
            return getFormattedReasonTextForFieldAssertion(assertion, run);
        case AssertionType.Sql:
            return getFormattedReasonTextForSqlAssertion(assertion, run);
        case AssertionType.Dataset:
            return getFormattedReasonTextForDefaultAssertion(assertion, run);
        default:
            return 'No reason provided';
    }
};

export function applyOpacityToHexColor(hex, opacity) {
    // Ensure the hex color is valid and remove any leading #
    const finalHex = hex.replace(/^#/, '');

    // Convert opacity from 0-1 range to 0-255 range
    const alpha = Math.round(opacity * 255);

    // Convert the alpha value to a hex string and ensure it's 2 characters long
    const alphaHex = (alpha + 0x100).toString(16).substr(-2);

    // Return the original hex color with the alpha opacity appended
    return `#${finalHex}${alphaHex}`;
}

export const getDetailedErrorMessage = (run: AssertionRunEvent) => {
    return run.result?.error?.properties?.find((property) => property.key === 'message')?.value || undefined;
};

export enum ResultStatusType {
    LATEST = 'latest',
    HISTORICAL = 'historical',
}

/**
 * Returns the display text assoociated with an AssertionResultType
 */
export const getResultStatusText = (result: AssertionResultType, type: ResultStatusType) => {
    switch (result) {
        case AssertionResultType.Success:
            return type === ResultStatusType.LATEST ? 'Passing' : 'Passed';
        case AssertionResultType.Failure:
            return type === ResultStatusType.LATEST ? 'Failing' : 'Failed';
        case AssertionResultType.Error:
            return 'Error';
        case AssertionResultType.Init:
            return type === ResultStatusType.LATEST ? 'Initializing' : 'Initialized';
        default:
            throw new Error(`Unsupported Assertion Result Type ${result} provided.`);
    }
};



/**
 * Gets the main metric on an assertion's results that are being monitored over time
 * @param runEvent 
 * @returns {number | undefined}
 */
export const tryGetPrimaryMetricValueFromAssertionRunEvent = (runEvent: AssertionRunEvent): number | undefined => {
    switch (runEvent.result?.assertion?.type) {
        case AssertionType.Sql:
            return tryGetSqlAssertionNumericalResult(runEvent.result);
        case AssertionType.Volume:
            return (runEvent.result.rowCount?.valueOf());
        case AssertionType.Field:
            return tryGetPrimaryMetricValueFromFieldAssertionRunEvent(runEvent.result)
        case AssertionType.Dataset:
            return undefined;
        case AssertionType.DataSchema:
            return undefined;
        case AssertionType.Freshness:
            return undefined;
        default:
            return undefined;
    }
}

function tryGetPrimaryMetricValueFromFieldAssertionRunEvent(runEventResult?: Maybe<AssertionResult>): number | undefined {
    switch (runEventResult?.assertion?.fieldAssertion?.type) {
        case FieldAssertionType.FieldValues: {
            return tryGetFieldValueAssertionNumericalResult(runEventResult)
        }
        case FieldAssertionType.FieldMetric: {
            return tryGetFieldMetricAssertionNumericalResult(runEventResult)
        }
        default:
            return undefined;
    }
}



export type AssertionExpectedRange = {
    high?: number
    low?: number
}


/**
 * Tries to get the 'high' and 'low' end of the range for an assertion
 * If both are defined we have a BETWEEN range
 * If either or are defined we only have a lt/lte/gt/gte range
 * If neither are defined, we cannot extract an expected range for this assertion run event
 * @param runEvent 
 * @returns {AssertionExpectedRange}
 */
export const tryGetExpectedRangeFromAssertionRunEvent = (runEvent: AssertionRunEvent): AssertionExpectedRange => {
    let result: AssertionExpectedRange = {}

    const info = runEvent.result?.assertion;
    switch (info?.type) {
        case AssertionType.Volume:
            result = info.volumeAssertion ? tryGetExpectedRangeFromVolumeAssertion(info.volumeAssertion, tryGetPreviousVolumeAssertionNumericalResult(runEvent.result)) : result;
            break;
        case AssertionType.Field:
            result = info.fieldAssertion ? tryGetExpectedRangeFromFieldAssertion(info.fieldAssertion) : result;
            break;
        case AssertionType.Sql:
            result = info.sqlAssertion ? tryGetExpectedRangeFromSQLAssertion(info.sqlAssertion, tryGetPreviousSqlAssertionNumericalResult(runEvent.result)) : result;
            break;
        default:
            break;
    }
    return result;
}

function tryGetExpectedRangeFromFieldAssertion(fieldAssertionInfo: FieldAssertionInfo): AssertionExpectedRange {
    let result: AssertionExpectedRange = {}
    switch (fieldAssertionInfo.type) {
        case FieldAssertionType.FieldValues:
            result = tryGetExpectedRangeFromAssertionAgainstTotals(fieldAssertionInfo.fieldValuesAssertion)
            break;
        case FieldAssertionType.FieldMetric:
            result = tryGetExpectedRangeFromAssertionAgainstTotals(fieldAssertionInfo.fieldMetricAssertion)
            break;
        default:
            break;
    }
    return result;
}

function tryGetExpectedRangeFromSQLAssertion(sqlAssertionInfo: SqlAssertionInfo, maybePreviousResult?: number): AssertionExpectedRange {
    if (!sqlAssertionInfo.changeType) {
        return tryGetExpectedRangeFromAssertionAgainstTotals(sqlAssertionInfo)
    }

    return tryGetExpectedRangeFromAssertionAgainstChanges(sqlAssertionInfo, sqlAssertionInfo.changeType, maybePreviousResult)
}

function tryGetExpectedRangeFromVolumeAssertion(volumeAssertionInfo: VolumeAssertionInfo, maybePreviousRowCount?: number): AssertionExpectedRange {
    let result: AssertionExpectedRange = {}

    switch (volumeAssertionInfo?.type) {
        case VolumeAssertionType.RowCountTotal: {
            result = tryGetExpectedRangeFromAssertionAgainstTotals(volumeAssertionInfo.rowCountTotal)
            break;
        }
        case VolumeAssertionType.IncrementingSegmentRowCountTotal: {
            result = tryGetExpectedRangeFromAssertionAgainstTotals(volumeAssertionInfo.incrementingSegmentRowCountTotal)
            break;
        }
        case VolumeAssertionType.RowCountChange:
            result = tryGetExpectedRangeFromAssertionAgainstChanges(volumeAssertionInfo.rowCountChange, volumeAssertionInfo.rowCountChange?.type, maybePreviousRowCount)
            break;
        case VolumeAssertionType.IncrementingSegmentRowCountChange:
            result = tryGetExpectedRangeFromAssertionAgainstChanges(volumeAssertionInfo.incrementingSegmentRowCountChange, volumeAssertionInfo.incrementingSegmentRowCountChange?.type, maybePreviousRowCount)
            break;
        default:
            break;
    }
    return result
}
function tryGetExpectedRangeFromAssertionAgainstChanges(changingInfo?: Maybe<RowCountChange | IncrementingSegmentRowCountChange | SqlAssertionInfo>, changeType?: Maybe<AssertionValueChangeType>, previousCount?: Maybe<number>): AssertionExpectedRange {
    let high: undefined | number;
    let low: undefined | number;

    if (!changingInfo?.parameters || typeof previousCount !== 'number' || typeof changeType === 'undefined' || changeType === null) {
        return { high, low };
    }


    switch (changingInfo?.operator) {
        case AssertionStdOperator.Between: {
            const modifierHigh = tryExtractNumericalValueFromAssertionStdParameter(changingInfo.parameters.maxValue)
            const modifierLow = tryExtractNumericalValueFromAssertionStdParameter(changingInfo.parameters.minValue)
            high = typeof modifierHigh === 'number' ? calculateExpectedNumericalValueWithPreviousNumericalValue(
                previousCount,
                changeType,
                modifierHigh
            ) : undefined
            low = typeof modifierLow === 'number' ? calculateExpectedNumericalValueWithPreviousNumericalValue(
                previousCount,
                changeType,
                modifierLow
            ) : undefined;
            break;
        }
        case AssertionStdOperator.GreaterThan: {
            const modifierLow = tryExtractNumericalValueFromAssertionStdParameter(changingInfo.parameters.value)
            low = typeof modifierLow === 'number' ? calculateExpectedNumericalValueWithPreviousNumericalValue(
                previousCount,
                changeType,
                modifierLow
            ) : undefined;
            break;
        }
        case AssertionStdOperator.GreaterThanOrEqualTo: {
            const modifierLow = tryExtractNumericalValueFromAssertionStdParameter(changingInfo.parameters.value)
            low = typeof modifierLow === 'number' ? calculateExpectedNumericalValueWithPreviousNumericalValue(
                previousCount,
                changeType,
                modifierLow
            ) : undefined;
            break;
        }
        case AssertionStdOperator.LessThan: {
            const modifierHigh = tryExtractNumericalValueFromAssertionStdParameter(changingInfo.parameters.value)
            high = typeof modifierHigh === 'number' ? calculateExpectedNumericalValueWithPreviousNumericalValue(
                previousCount,
                changeType,
                modifierHigh
            ) : undefined;
            break;
        }
        case AssertionStdOperator.LessThanOrEqualTo: {
            const modifierHigh = tryExtractNumericalValueFromAssertionStdParameter(changingInfo.parameters.value)
            high = typeof modifierHigh === 'number' ? calculateExpectedNumericalValueWithPreviousNumericalValue(
                previousCount,
                changeType,
                modifierHigh
            ) : undefined;
            break;
        }
        default:
            break;
    }
    return { high, low };
}
function tryGetExpectedRangeFromAssertionAgainstTotals(totals?: Maybe<IncrementingSegmentRowCountTotal> | Maybe<RowCountTotal> | Maybe<SqlAssertionInfo> | Maybe<FieldValuesAssertion> | Maybe<FieldMetricAssertion>): AssertionExpectedRange {
    let high: undefined | number;
    let low: undefined | number;
    if (!totals?.parameters) {
        return { high, low };
    }
    switch (totals?.operator) {
        case AssertionStdOperator.Between:
            high = tryExtractNumericalValueFromAssertionStdParameter(totals.parameters.maxValue)
            low = tryExtractNumericalValueFromAssertionStdParameter(totals.parameters.minValue)
            break;
        case AssertionStdOperator.GreaterThan:
            low = tryExtractNumericalValueFromAssertionStdParameter(totals.parameters.value)
            break;
        case AssertionStdOperator.GreaterThanOrEqualTo:
            low = tryExtractNumericalValueFromAssertionStdParameter(totals.parameters.value)
            break;
        case AssertionStdOperator.LessThan:
            high = tryExtractNumericalValueFromAssertionStdParameter(totals.parameters.value)
            break;
        case AssertionStdOperator.LessThanOrEqualTo:
            high = tryExtractNumericalValueFromAssertionStdParameter(totals.parameters.value)
            break;
        default:
            break;
    }
    return { high, low };
}