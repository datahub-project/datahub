import {
    Assertion,
    AssertionResultType,
    AssertionRunEvent,
    AssertionType,
    FieldAssertionType,
    SqlAssertionType,
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

const getActualColumnMetricFromAssertionRunEvent = (run: AssertionRunEvent): number | undefined => {
    // TODO: move hardcoded keys to {@link ASSERTION_RESULT__NATIVE_RESULTS__KEYS_BY_ASSERTION_TYPE}
    const stringMetric = run?.result?.nativeResults?.find((pair) => pair.key === 'Metric Value')?.value;
    return stringMetric ? parseInt(stringMetric, 10) : undefined;
};

const getActualSqlResultFromAssertionRunEvent = (run: AssertionRunEvent) => {
    const stringResult = run?.result?.nativeResults?.find((pair) => pair.key === 'Value')?.value;
    return stringResult ? parseInt(stringResult, 10) : undefined;
};

const getPreviousSqlResultFromAssertionRunEvent = (run: AssertionRunEvent) => {
    const stringResult = run?.result?.nativeResults?.find((pair) => pair.key === 'Previous Value')?.value;
    return stringResult ? parseInt(stringResult, 10) : undefined;
};

const getActualInvalidRowCountFromAssertionRunEvent = (run: AssertionRunEvent) => {
    const stringRowCount = run?.result?.nativeResults?.find((pair) => pair.key === 'Invalid Rows')?.value;
    return stringRowCount ? parseInt(stringRowCount, 10) : undefined;
};

const getActualUpdatedTimestampFromAssertionRunEvent = (run: AssertionRunEvent) => {
    const eventsArrString = run?.result?.nativeResults?.find((pair) => pair.key === 'events')?.value;
    const eventsArr = eventsArrString ? JSON.parse(eventsArrString) : [];
    const timestamps = eventsArr.map((event) => event.time);
    const sortedTimestampsDescending = timestamps.sort((a, b) => b - a);
    const maxTimestamp = sortedTimestampsDescending[0];
    return maxTimestamp;
};

const getActualRowCountFromAssertionRunEvent = (run: AssertionRunEvent) => {
    const rowCount = run?.result?.rowCount;
    return rowCount || undefined;
};

const getPreviousRowCountFromAssertionRunEvent = (run: AssertionRunEvent) => {
    const previousRowCountStr = run?.result?.nativeResults?.find((pair) => pair.key === 'Previous Row Count')?.value;
    return previousRowCountStr ? parseInt(previousRowCountStr, 10) : undefined;
};

// TODO: Consider supporting relative field metric assertions.
const getFormattedReasonTextForFieldMetricAssertion = (assertion: Assertion, run: AssertionRunEvent) => {
    const field = assertion.info?.fieldAssertion?.fieldMetricAssertion?.field?.path || 'column';
    const metricType = assertion.info?.fieldAssertion?.fieldMetricAssertion?.metric;
    const metricText = (metricType && getFieldMetricLabel(metricType)) || 'Aggregation';
    const actual = getActualColumnMetricFromAssertionRunEvent(run);
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
    const invalidRowsCount = getActualInvalidRowCountFromAssertionRunEvent(run);
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
    const actualRowCount = getActualSqlResultFromAssertionRunEvent(run);
    const previousRowCount = getPreviousSqlResultFromAssertionRunEvent(run);
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
    const actualTimestamp = getActualUpdatedTimestampFromAssertionRunEvent(run);
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
    const actualRowCount = getActualRowCountFromAssertionRunEvent(run);
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
    const actualRowCount = getActualRowCountFromAssertionRunEvent(run);
    const previousRowCount = getPreviousRowCountFromAssertionRunEvent(run);
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
            // TODO(jayacryl): check if accurate
            return (runEvent.result.actualAggValue?.valueOf());
        case AssertionType.Volume:
            // Row count
            return (runEvent.result.rowCount?.valueOf());
        case AssertionType.Field:
            switch (runEvent.result.assertion.fieldAssertion?.type) {
                case FieldAssertionType.FieldValues:
                    // Invalid rows
                    {
                        if (runEvent.result.type === AssertionResultType.Init) return 0;
                        const maybeValue = runEvent.result.nativeResults?.find(result => result.key === ASSERTION_NATIVE_RESULTS_KEYS_BY_ASSERTION_TYPE.FIELD_ASSERTIONS.FIELD_VALUES.Y_VALUE_KEY_NAME)?.value
                        const parsedValue = typeof maybeValue === 'string' ? parseFloat(maybeValue) : maybeValue
                        return typeof parsedValue === 'number' && !Number.isNaN(parsedValue) ? parsedValue : undefined;
                    }
                case FieldAssertionType.FieldMetric:
                    // Metric value
                    {
                        const maybeValue = runEvent.result.nativeResults?.find(result => result.key === ASSERTION_NATIVE_RESULTS_KEYS_BY_ASSERTION_TYPE.FIELD_ASSERTIONS.METRIC_VALUES.Y_VALUE_KEY_NAME)?.value
                        const parsedValue = typeof maybeValue === 'string' ? parseFloat(maybeValue) : maybeValue
                        return typeof parsedValue === 'number' && !Number.isNaN(parsedValue) ? parsedValue : undefined;
                    }
                default:
                    break;
            }
            break;
        case AssertionType.Dataset:
            break;
        case AssertionType.DataSchema:
            break;
        case AssertionType.Freshness:
            break;
        default:
            break;
    }
    return undefined;
}
