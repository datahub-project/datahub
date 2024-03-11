import { Assertion, AssertionResultType, AssertionRunEvent, AssertionType, FieldAssertionType, FreshnessAssertionScheduleType, SqlAssertionType } from "../../../../../../../../../../types.generated";
import { formatNumberWithoutAbbreviation } from "../../../../../../../../../shared/formatNumber";
import { toLocalDateString, toLocalTimeString } from "../../../../../../../../../shared/time/timeUtils";
import { getResultErrorMessage } from "../../../../assertionUtils";
import { getFieldMetricLabel } from "../../../builder/steps/field/utils";
import { tryGetAbsoluteVolumeAssertionNumericalResult, tryGetActualUpdatedTimestampFromAssertionResult, tryGetFieldMetricAssertionNumericalResult, tryGetFieldValueAssertionNumericalResult, tryGetPreviousSqlAssertionNumericalResult, tryGetPreviousVolumeAssertionNumericalResult, tryGetSqlAssertionNumericalResult } from "./resultExtractionUtils";
import { getCronAsText } from '../../../../acrylUtils';

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

const calculateMetricChangePercentage = (previous: number, actual: number) => {
    return previous ? ((actual - previous) / previous) * 100 : undefined;
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
            return `Dataset was updated at ${formattedTime} on ${formattedDate}.`;
        }
        return `The dataset was updated within the expected timeframe.`;
    }
    return `No dataset updates occurred within the expected timeframe.`;
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


const getFormattedExpectedTextForFreshnessAssertion = (run: AssertionRunEvent): string | undefined => {
    const info = run.result?.assertion?.freshnessAssertion
    if (!info) return undefined;
    switch (info.schedule.type) {
        case FreshnessAssertionScheduleType.Cron: {
            if (!info.schedule.cron) return undefined;
            const humanReadableCronStr = getCronAsText(info.schedule.cron.cron).text
            const maybeTimeZoneStr = info.schedule.cron.timezone ? ` (${info.schedule.cron.timezone})` : ``;
            const maybeWindowOffsetStr = info.schedule.cron.windowStartOffsetMs ? ` with a window offset of ${info.schedule.cron.windowStartOffsetMs} millis` : ``;
            return `Expected dataset to update before the assertion ran ${humanReadableCronStr}${maybeTimeZoneStr}${maybeWindowOffsetStr}`;
        }
        case FreshnessAssertionScheduleType.FixedInterval: {
            if (!info.schedule.fixedInterval) return undefined;
            return `Expected dataset to update within the last ${info.schedule.fixedInterval.multiple} ${info.schedule.fixedInterval.unit.valueOf().toLowerCase()}${info.schedule.fixedInterval.multiple === 1 ? '' : 's'}`;
        }
        default:
            return undefined;
    }
}

const getFormattedExpectedTextForVolumeAssertion = (_: AssertionRunEvent): string | undefined => {
    return undefined;
}

const getFormattedExpectedTextForFieldAssertion = (_: AssertionRunEvent): string | undefined => {
    return undefined;
}

const getFormattedExpectedTextForSqlAssertion = (_: AssertionRunEvent): string | undefined => {
    return undefined;
}

const getFormattedExpectedTextForDefaultAssertion = (_: AssertionRunEvent): string | undefined => {
    return undefined;
}

export const getFormattedExpectedResultText = (_: AssertionRunEvent): string | undefined => {
    switch (run.result?.assertion?.type) {
        case AssertionType.Freshness:
            return getFormattedExpectedTextForFreshnessAssertion(run);
        case AssertionType.Volume:
            return getFormattedExpectedTextForVolumeAssertion(run);
        case AssertionType.Field:
            return getFormattedExpectedTextForFieldAssertion(run);
        case AssertionType.Sql:
            return getFormattedExpectedTextForSqlAssertion(run);
        case AssertionType.Dataset:
            return getFormattedExpectedTextForDefaultAssertion(run);
        default:
            return undefined;
    }
};


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

