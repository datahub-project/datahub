import _ from 'lodash';

import { getCronAsText } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { getFieldMetricLabel } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/field/utils';
import { toReadableLocalDateTimeString } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/shared/utils';
import {
    ASSERTION_OPERATOR_DESCRIPTIONS_REQUIRING_SUFFIX,
    GET_ASSERTION_OPERATOR_TO_DESCRIPTION_MAP,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/constants';
import {
    AssertionExpectedRange,
    tryGetAbsoluteVolumeAssertionNumericalResult,
    tryGetActualUpdatedTimestampFromAssertionResult,
    tryGetExpectedRangeFromAssertionAgainstAbsoluteValues,
    tryGetExpectedRangeFromAssertionAgainstRelativeValues,
    tryGetExpectedRangeFromFailThreshold,
    tryGetExtraFieldsInActual,
    tryGetExtraFieldsInExpected,
    tryGetFieldMetricAssertionNumericalResult,
    tryGetFieldValueAssertionNumericalResult,
    tryGetMismatchedTypeFields,
    tryGetPreviousSqlAssertionNumericalResult,
    tryGetPreviousVolumeAssertionNumericalResult,
    tryGetSqlAssertionNumericalResult,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/resultExtractionUtils';
import { getResultErrorMessage } from '@app/entityV2/shared/tabs/Dataset/Validations/assertionUtils';
import { getFieldMetricTypeReadableLabel } from '@app/entityV2/shared/tabs/Dataset/Validations/fieldDescriptionUtils';
import { NUMBER_DISPLAY_PRECISION } from '@app/entityV2/shared/tabs/Dataset/Validations/shared/constant';
import { formatNumberWithoutAbbreviation } from '@app/shared/formatNumber';
import { lowerFirstLetter } from '@app/shared/textUtil';
import { toLocalDateString, toLocalTimeString } from '@app/shared/time/timeUtils';

import {
    Assertion,
    AssertionInfo,
    AssertionResult,
    AssertionResultType,
    AssertionRunEvent,
    AssertionSourceType,
    AssertionType,
    AssertionValueChangeType,
    FieldAssertionInfo,
    FieldAssertionType,
    FieldMetricAssertion,
    FieldValuesAssertion,
    FreshnessAssertionScheduleType,
    IncrementingSegmentRowCountChange,
    IncrementingSegmentRowCountTotal,
    Maybe,
    RowCountChange,
    RowCountTotal,
    SqlAssertionInfo,
    SqlAssertionType,
    VolumeAssertionType,
} from '@types';

export const getFormattedResultText = (result?: AssertionResultType, isSmartAssertion?: boolean) => {
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
        return isSmartAssertion ? 'Training' : 'Initializing';
    }
    return undefined;
};

// TODO: Consider supporting relative field metric assertions.
const getFormattedReasonTextForFieldMetricAssertion = (run: AssertionRunEvent) => {
    const assertionInfo = run.result?.assertion;
    const field = assertionInfo?.fieldAssertion?.fieldMetricAssertion?.field?.path || 'column';
    const metricType = assertionInfo?.fieldAssertion?.fieldMetricAssertion?.metric;
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

const getFormattedReasonTextForFieldValuesAssertion = (run: AssertionRunEvent) => {
    const assertionInfo = run.result?.assertion;
    const field = assertionInfo?.fieldAssertion?.fieldValuesAssertion?.field?.path || 'column';
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

const getFormattedReasonTextForFieldAssertion = (run: AssertionRunEvent) => {
    const field = run.result?.assertion?.fieldAssertion;
    if (field?.type === FieldAssertionType.FieldMetric) {
        return getFormattedReasonTextForFieldMetricAssertion(run);
    }
    return getFormattedReasonTextForFieldValuesAssertion(run);
};

const getFormattedReasonTextForAbsoluteSqlAssertion = (run: AssertionRunEvent) => {
    // Be careful about showing the actual result that was returned, since it may contain sensitive information.
    const resultType = run.result?.type;
    const maybeResultValue = tryGetSqlAssertionNumericalResult(run.result);
    const resultValueStr = maybeResultValue ? ` (${maybeResultValue})` : '';
    if (resultType === AssertionResultType.Success) {
        return `The result of the provided SQL query${resultValueStr} met the expected conditions.`;
    }
    return `The result of the provided SQL query${resultValueStr} did not meet the expected conditions.`;
};

const calculateMetricChangePercentage = (previous: number, actual: number) => {
    return previous ? ((actual - previous) / previous) * 100 : undefined;
};
const getFormattedReasonTextForRelativeSqlAssertion = (run: AssertionRunEvent) => {
    const result = run.result?.type;
    const actualRowCount = tryGetSqlAssertionNumericalResult(run.result);
    const previousRowCount = tryGetPreviousSqlAssertionNumericalResult(run.result);
    const rowCountChangePercentage =
        actualRowCount && previousRowCount && calculateMetricChangePercentage(previousRowCount, actualRowCount);
    const rowCountChangeTotal = actualRowCount && previousRowCount ? actualRowCount - previousRowCount : 0;
    const positiveChange = (rowCountChangeTotal || 0) >= 0;
    const rowCountChangeText = `${formatNumberWithoutAbbreviation(rowCountChangeTotal)} (${
        positiveChange ? '+' : '-'
    }${rowCountChangePercentage}%)`;

    if (result === AssertionResultType.Success) {
        if (actualRowCount !== undefined && previousRowCount !== undefined) {
            return `The change in the SQL result of ${rowCountChangeText} met the expected conditions.`;
        }
        return `The change in the SQL result met the expected conditions.`;
    }
    if (actualRowCount !== undefined && previousRowCount !== undefined) {
        return `The change in the SQL result of ${rowCountChangeText} did not meet the expected conditions.`;
    }
    return `The change in the SQL result did not meet the expected conditions.`;
};

const getFormattedReasonTextForSqlAssertion = (run: AssertionRunEvent) => {
    const isAbsolute = run.result?.assertion?.sqlAssertion?.type === SqlAssertionType.Metric;
    return isAbsolute
        ? getFormattedReasonTextForAbsoluteSqlAssertion(run)
        : getFormattedReasonTextForRelativeSqlAssertion(run);
};

const getFormattedReasonTextForSchemaAssertion = (run: AssertionRunEvent) => {
    if (run.result?.type === AssertionResultType.Success) {
        return `The actual columns match the expected columns!`;
    }

    const extraFieldsInActual = tryGetExtraFieldsInActual(run.result) || [];
    const extraFieldsInExpected = tryGetExtraFieldsInExpected(run.result) || [];
    const mismatchedTypeFields = tryGetMismatchedTypeFields(run.result) || [];

    let reasonMessage = '';

    if (extraFieldsInActual.length > 0) {
        reasonMessage += `Found unexpected columns: ${extraFieldsInActual.join(', ')}. `;
    }
    if (extraFieldsInExpected.length > 0) {
        reasonMessage += `Missing expected columns: ${extraFieldsInExpected.join(', ')}. `;
    }
    if (mismatchedTypeFields.length > 0) {
        reasonMessage += `The expected and actual data types for the following columns do not match: ${mismatchedTypeFields.join(
            ', ',
        )}. `;
    }

    if (reasonMessage === '') {
        return 'The actual columns do not match the expected columns!';
    }

    return reasonMessage;
};

const getFormattedReasonTextForFreshnessAssertion = (run: AssertionRunEvent) => {
    // Be careful about showing the actual result that was returned, since it may contain sensitive information.
    const result = run.result?.type;
    const actualTimestamp = tryGetActualUpdatedTimestampFromAssertionResult(run.result);
    const formattedTime = (actualTimestamp && toLocalTimeString(actualTimestamp)) || undefined;
    const formattedDate = (actualTimestamp && toLocalDateString(actualTimestamp)) || undefined;
    if (result === AssertionResultType.Success) {
        if (formattedTime) {
            return `Table was updated at ${formattedTime} on ${formattedDate}.`;
        }
        return `The table was updated within the expected timeframe.`;
    }
    return `No table updates occurred within the expected timeframe.`;
};

const getFormattedReasonTextForAbsoluteVolumeAssertion = (run: AssertionRunEvent) => {
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

const getFormattedReasonTextForRelativeVolumeAssertion = (run: AssertionRunEvent) => {
    const result = run.result?.type;
    const actualRowCount = tryGetAbsoluteVolumeAssertionNumericalResult(run.result);
    const previousRowCount = tryGetPreviousVolumeAssertionNumericalResult(run.result);
    const rowCountChangePercentage =
        actualRowCount && previousRowCount && calculateMetricChangePercentage(previousRowCount, actualRowCount);
    const rowCountChangeTotal = actualRowCount && previousRowCount ? actualRowCount - previousRowCount : 0;
    const positiveChange = (rowCountChangeTotal || 0) >= 0;
    const rowCountChangeText = `${formatNumberWithoutAbbreviation(rowCountChangeTotal)} (${
        positiveChange ? '+' : '-'
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

const getFormattedReasonTextForVolumeAssertion = (run: AssertionRunEvent) => {
    const assertionInfo = run.result?.assertion;
    const isAbsolute = !!assertionInfo?.volumeAssertion?.rowCountTotal;
    return isAbsolute
        ? getFormattedReasonTextForAbsoluteVolumeAssertion(run)
        : getFormattedReasonTextForRelativeVolumeAssertion(run);
};

const getFormattedReasonTextForDefaultAssertion = (run: AssertionRunEvent) => {
    const type = run.result?.type;
    switch (type) {
        case AssertionResultType.Success: {
            return `The expected conditions were met`;
        }
        case AssertionResultType.Error: {
            let message = `Assertion encountered an error during execution.`;

            const maybeError = run.result?.error;
            const maybeType = maybeError?.type;
            const maybeErrorMessage = maybeError?.properties?.find((e) => e.key === 'message')?.value;
            if (maybeType) {
                message = `Assertion execution encountered an error with type ${maybeType}.`;
            }
            if (maybeErrorMessage) {
                message += ` "${maybeErrorMessage}"`;
            }
            return message;
        }
        default: {
            return `The expected conditions were not met.`;
        }
    }
};

export const getFormattedReasonText = (assertion: Assertion, run: AssertionRunEvent) => {
    if (run?.result?.type === AssertionResultType.Init) {
        return assertion.info?.source?.type === AssertionSourceType.Inferred
            ? 'Collecting data to train the model. Evaluation will begin once training is complete. This can take up to 7 days.'
            : 'Initial data recorded successfully. Assertion result will be available on the next evaluation.';
    }
    if (run?.result?.type === AssertionResultType.Error) {
        const formattedError = getResultErrorMessage(run?.result);
        return `${formattedError}`;
    }

    // Some historical assertion results may not have asseriton info...
    // so we coalesce the current info onto there to avoid blanks
    const coalescedResult: AssertionResult | undefined | null = run.result && {
        ...run.result,
        assertion: run.result?.assertion ?? assertion.info,
    };
    const coalescedRun: AssertionRunEvent = {
        ...run,
        result: coalescedResult,
    };
    switch (assertion.info?.type) {
        case AssertionType.Freshness:
            return getFormattedReasonTextForFreshnessAssertion(coalescedRun);
        case AssertionType.Volume:
            return getFormattedReasonTextForVolumeAssertion(coalescedRun);
        case AssertionType.Field:
            return getFormattedReasonTextForFieldAssertion(coalescedRun);
        case AssertionType.Sql:
            return getFormattedReasonTextForSqlAssertion(coalescedRun);
        case AssertionType.DataSchema:
            return getFormattedReasonTextForSchemaAssertion(coalescedRun);
        case AssertionType.Dataset:
            return getFormattedReasonTextForDefaultAssertion(coalescedRun);
        case AssertionType.Custom:
            return getFormattedReasonTextForDefaultAssertion(coalescedRun);
        default:
            return 'No reason provided';
    }
};

const getFormattedExpectedResultTextForAbsoluteAssertionRange = (
    assertedOnDescription: string,
    range: AssertionExpectedRange,
): string | undefined => {
    let { low, high } = range;
    low = low && formatNumberWithoutAbbreviation(low);
    high = high && formatNumberWithoutAbbreviation(high);

    let message: string | undefined;
    const isHighValid = typeof high !== 'undefined';
    const isLowValid = typeof low !== 'undefined';
    if (isHighValid && isLowValid) {
        message = `${assertedOnDescription} should be between ${low} and ${high}.`;
    } else if (isHighValid) {
        const rangeDefinition = range.context?.highType === 'inclusive' ? 'less than or equal to' : 'less than';
        message = `${assertedOnDescription} should be ${rangeDefinition} ${high}.`;
    } else if (isLowValid) {
        const rangeDefinition = range.context?.lowType === 'inclusive' ? 'greater than or equal to' : 'greater than';
        message = `${assertedOnDescription} should be ${rangeDefinition} ${low}.`;
    }

    return message;
};

/**
 * Gets formatted text describing assertion expectations for absolute assertions
 * @param assertedOnDescription: for prefixing the output
 * @param totalsInfo: TODO just point out the specific info we need (ie min/max/value/operator), let the parent extract that and pass it in
 */
const getFormattedExpectedTextForAbsoluteAssertion = (
    assertedOnDescription: string,
    totalsInfo?:
        | Maybe<IncrementingSegmentRowCountTotal>
        | Maybe<RowCountTotal>
        | Maybe<SqlAssertionInfo>
        | Maybe<FieldValuesAssertion>
        | Maybe<FieldMetricAssertion>,
): string | undefined => {
    const range = tryGetExpectedRangeFromAssertionAgainstAbsoluteValues(totalsInfo);
    return getFormattedExpectedResultTextForAbsoluteAssertionRange(assertedOnDescription, range);
};

const getFormattedExpectedResultTextForValueAssertion = (
    assertedOnDescription: string,
    totalsInfo?: Maybe<FieldValuesAssertion>,
): string | undefined => {
    let message: string | undefined;

    // 1. Handle cases where there is a range (ie. between, less than, etc.)
    message = getFormattedExpectedTextForAbsoluteAssertion(assertedOnDescription, totalsInfo);
    if (message) {
        return message;
    }
    const ASSERTION_OPERATOR_TO_DESCRIPTION = GET_ASSERTION_OPERATOR_TO_DESCRIPTION_MAP({ isPlural: false });

    // 2. Handle cases where there is a more explicit expectation (ie. 'contains XYZ', 'is equal to 5')
    if (totalsInfo?.operator && ASSERTION_OPERATOR_TO_DESCRIPTION[totalsInfo.operator]) {
        const operatorDescription = ASSERTION_OPERATOR_TO_DESCRIPTION[totalsInfo.operator];
        // ie. 'in [red, blue, green]'
        if (ASSERTION_OPERATOR_DESCRIPTIONS_REQUIRING_SUFFIX.includes(totalsInfo.operator)) {
            const value = totalsInfo.parameters?.value?.value ?? '[configured value]'; // Should never happen but a graceful fallback on the UX
            message = `${assertedOnDescription} ${lowerFirstLetter(operatorDescription)} ${value}.`;
        }
        // ie. 'is not null'
        message = `${assertedOnDescription} ${lowerFirstLetter(operatorDescription)}.`;
    }

    return message;
};

/**
 * Gets formatted text describing assertion expectations for relative assertions
 * @param assertedOnDescription: for prefixing the output
 * @param changingInfo: TODO just point out the specific info we need (ie min/max/value/operator), let the parent extract that and pass it in
 * @param changeType: NOTE: we don't extract this from changingInfo because {@link SqlAssertionInfo} calls it 'changeType' instead of 'type'
 */
const getFormattedExpectedTextForRelativeAssertion = (
    assertedOnDescription: string,
    changingInfo?: Maybe<RowCountChange | IncrementingSegmentRowCountChange | SqlAssertionInfo>,
    changeType?: Maybe<AssertionValueChangeType>,
    previousCount?: Maybe<number>,
): string | undefined => {
    if (
        !changingInfo?.parameters ||
        typeof previousCount !== 'number' ||
        typeof changeType === 'undefined' ||
        changeType === null
    ) {
        return undefined;
    }

    const range = tryGetExpectedRangeFromAssertionAgainstRelativeValues(changingInfo, changeType, previousCount);
    const { relativeModifiers } = range.context ?? {};

    const rangeSuffix = changeType === AssertionValueChangeType.Percentage ? '%' : '';
    const rangeHighLabelSign = (relativeModifiers?.high ?? 0) >= 0 ? '+' : '-';
    const maybeRangeHighLabel =
        typeof relativeModifiers?.high === 'number'
            ? `${rangeHighLabelSign}${relativeModifiers.high}${rangeSuffix}`
            : undefined;

    const rangeLowLabelSign = (relativeModifiers?.low ?? 0) >= 0 ? '+' : '-';
    const maybeRangeLowLabel =
        typeof relativeModifiers?.low === 'number'
            ? `${rangeLowLabelSign}${relativeModifiers.low}${rangeSuffix}`
            : undefined;

    let { low, high } = range;
    low = low && formatNumberWithoutAbbreviation(Math.floor(low));
    high = high && formatNumberWithoutAbbreviation(Math.floor(high));

    const isHighValid = typeof high !== 'undefined';
    const isLowValid = typeof low !== 'undefined';

    if (isHighValid && isLowValid) {
        const minuteDetails =
            maybeRangeHighLabel && maybeRangeLowLabel ? ` (${maybeRangeLowLabel} to ${maybeRangeHighLabel})` : '';
        return `${assertedOnDescription} should be between ${low} and ${high}${minuteDetails}.`;
    }
    if (isHighValid) {
        const minuteDetails = maybeRangeHighLabel ? ` (${maybeRangeHighLabel})` : '';
        const rangeDefinition = range.context?.highType === 'inclusive' ? 'less than or equal to' : 'less than';
        return `${assertedOnDescription} should be ${rangeDefinition} ${high}${minuteDetails}.`;
    }
    if (isLowValid) {
        const minuteDetails = maybeRangeLowLabel ? ` (${maybeRangeLowLabel})` : '';
        const rangeDefinition = range.context?.lowType === 'inclusive' ? 'greater than or equal to' : 'greater than';
        return `${assertedOnDescription} should be ${rangeDefinition} ${low}${minuteDetails}.`;
    }
    return undefined;
};

const getFormattedExpectedTextForVolumeAssertion = (
    run: AssertionRunEvent | undefined,
    assertion: AssertionInfo,
): string | undefined => {
    const volumeAssertion = run?.result?.assertion?.volumeAssertion || assertion?.volumeAssertion;
    switch (volumeAssertion?.type) {
        case VolumeAssertionType.RowCountChange:
            if (!run) return undefined; // inferred assertions don't use rowCountChange, so for future predictions we don't need to handle this case
            return getFormattedExpectedTextForRelativeAssertion(
                'Row count',
                volumeAssertion.rowCountChange,
                volumeAssertion.rowCountChange?.type,
                tryGetPreviousVolumeAssertionNumericalResult(run.result),
            );
        case VolumeAssertionType.RowCountTotal:
            return getFormattedExpectedTextForAbsoluteAssertion('Row count', volumeAssertion.rowCountTotal);
        default:
            return undefined;
    }
};

const getFormattedExpectedTextForFieldValuesAssertion = (
    fieldAssertionInfo: FieldAssertionInfo,
): string | undefined => {
    const maybeColumnPath = fieldAssertionInfo.fieldValuesAssertion?.field?.path;
    const fieldDescription = maybeColumnPath ? `${maybeColumnPath}` : `Column value`;
    const valueExpectationText = getFormattedExpectedResultTextForValueAssertion(
        `Row is valid if ${fieldDescription}`,
        fieldAssertionInfo.fieldValuesAssertion,
    );

    const range = tryGetExpectedRangeFromFailThreshold(fieldAssertionInfo.fieldValuesAssertion);
    const failingExpectedRowCountRangeText = getFormattedExpectedResultTextForAbsoluteAssertionRange(
        'Invalid row count',
        range,
    );

    if (failingExpectedRowCountRangeText && valueExpectationText) {
        return `${failingExpectedRowCountRangeText} ${valueExpectationText}`;
    }
    return valueExpectationText || failingExpectedRowCountRangeText;
};
const getFormattedExpectedTextForFieldMetricsAssertion = (
    fieldAssertionInfo: FieldAssertionInfo,
): string | undefined => {
    const maybeColumnPath = fieldAssertionInfo.fieldMetricAssertion?.field?.path;
    let metricDescription: string = maybeColumnPath ? `Metric of ${maybeColumnPath}` : 'Column metric';

    if (fieldAssertionInfo.fieldMetricAssertion?.metric) {
        try {
            const maybeMetricDescription = getFieldMetricTypeReadableLabel(
                fieldAssertionInfo.fieldMetricAssertion.metric,
            );
            // ie. 'Null percentage of age_m'
            metricDescription = maybeMetricDescription
                ? `${maybeMetricDescription} of ${maybeColumnPath ?? 'column'}`
                : metricDescription;
        } catch (e) {
            // best attempt
        }
    }
    return getFormattedExpectedTextForAbsoluteAssertion(metricDescription, fieldAssertionInfo.fieldMetricAssertion);
};
const getFormattedExpectedTextForFieldAssertion = (
    run: AssertionRunEvent | undefined,
    assertion: AssertionInfo,
): string | undefined => {
    const fieldAssertionInfo = run?.result?.assertion?.fieldAssertion || assertion?.fieldAssertion;
    if (!fieldAssertionInfo) return undefined;
    switch (fieldAssertionInfo.type) {
        case FieldAssertionType.FieldValues:
            return getFormattedExpectedTextForFieldValuesAssertion(fieldAssertionInfo);
        case FieldAssertionType.FieldMetric:
            return getFormattedExpectedTextForFieldMetricsAssertion(fieldAssertionInfo);
        default:
            return undefined;
    }
};

const getFormattedExpectedTextForSqlAssertion = (
    run: AssertionRunEvent | undefined,
    assertion: AssertionInfo,
): string | undefined => {
    const sqlAssertionInfo = run?.result?.assertion?.sqlAssertion || assertion?.sqlAssertion;
    if (!sqlAssertionInfo) return undefined;

    return sqlAssertionInfo.changeType
        ? // NOTE: inferred assertions don't use sqlAssertionInfo.changeType, so for future predictions we don't need to handle this case
          run &&
              getFormattedExpectedTextForRelativeAssertion(
                  'SQL result',
                  sqlAssertionInfo,
                  sqlAssertionInfo.changeType,
                  tryGetPreviousSqlAssertionNumericalResult(run.result),
              )
        : getFormattedExpectedTextForAbsoluteAssertion('SQL result', sqlAssertionInfo);
};

const getFormattedExpectedTextForFreshnessAssertion = (
    run: AssertionRunEvent | undefined,
    assertion: AssertionInfo,
): string | undefined => {
    const info = run?.result?.assertion?.freshnessAssertion || assertion?.freshnessAssertion;
    if (!info) return undefined;
    // NOTE: this should always be defined for an assertion run
    switch (info.schedule?.type) {
        case FreshnessAssertionScheduleType.Cron: {
            if (!info.schedule?.cron) return undefined;
            const humanReadableCronStr = getCronAsText(info.schedule.cron.cron, { verbose: true }).text;
            const maybeTimeZoneStr = info.schedule.cron.timezone ? ` (${info.schedule.cron.timezone})` : ``;
            const maybeWindowOffsetStr = info.schedule.cron.windowStartOffsetMs
                ? ` with a window offset of ${info.schedule.cron.windowStartOffsetMs} millis`
                : ``;
            return `Table should update between the scheduled cron windows. The cron set the check to run ${humanReadableCronStr}${maybeTimeZoneStr}${maybeWindowOffsetStr}`;
        }
        case FreshnessAssertionScheduleType.SinceTheLastCheck: {
            return `Table should update since the last time this check was run.`;
        }
        case FreshnessAssertionScheduleType.FixedInterval: {
            if (!info.schedule.fixedInterval) return undefined;
            return `Table should update within the last ${
                info.schedule.fixedInterval.multiple
            } ${info.schedule.fixedInterval.unit.valueOf().toLowerCase()}${
                info.schedule.fixedInterval.multiple === 1 ? '' : 's'
            } as of ${run ? toReadableLocalDateTimeString(run.timestampMillis) : 'next run'}.`;
        }
        default:
            return undefined;
    }
};

const getFormattedExpectedTextForDefaultAssertion = (_unused: AssertionRunEvent): string | undefined => {
    return undefined;
};

export const getFormattedExpectedResultText = (
    assertion?: Maybe<AssertionInfo>,
    run?: AssertionRunEvent,
): string | undefined => {
    if (assertion?.source?.type === AssertionSourceType.Inferred && run?.result?.type === AssertionResultType.Init) {
        return undefined;
    }
    // Some historical assertion results may not have asseriton info...
    // but we don't coalesce 'assertion' into the run.result.assertion because the exepectation
    // at the time of this run event may have been different than what's currently on the asseriton
    const fallbackAssertionInfo = assertion ?? run?.result?.assertion;
    if (!fallbackAssertionInfo) return undefined;
    switch (run?.result?.assertion?.type ?? assertion?.type) {
        case AssertionType.Freshness:
            return getFormattedExpectedTextForFreshnessAssertion(run, fallbackAssertionInfo);
        case AssertionType.Volume:
            return getFormattedExpectedTextForVolumeAssertion(run, fallbackAssertionInfo);
        case AssertionType.Field:
            return getFormattedExpectedTextForFieldAssertion(run, fallbackAssertionInfo);
        case AssertionType.Sql:
            return getFormattedExpectedTextForSqlAssertion(run, fallbackAssertionInfo);
        case AssertionType.Dataset:
            if (!run) return undefined;
            return getFormattedExpectedTextForDefaultAssertion(run);
        default:
            return undefined;
    }
};

export const getFormattedActualVsExpectedTextForVolumeAssertion = (
    run: AssertionRunEvent,
):
    | {
          actualText: string;
          expectedLowText?: string;
          expectedHighText?: string;
          expectedLowTextWithDecimals?: string;
          expectedHighTextWithDecimals?: string;
      }
    | undefined => {
    const isError = run?.result?.type === AssertionResultType.Error;

    const actualRowCount = tryGetAbsoluteVolumeAssertionNumericalResult(run?.result);
    const formattedActual = isError
        ? ''
        : formatNumberWithoutAbbreviation(actualRowCount ? Math.round(actualRowCount) : 0);

    const volumeAssertion = run?.result?.assertion?.volumeAssertion;

    const range = volumeAssertion?.rowCountTotal
        ? tryGetExpectedRangeFromAssertionAgainstAbsoluteValues(volumeAssertion.rowCountTotal)
        : undefined;
    const expectedLowText = range?.low != null ? formatNumberWithoutAbbreviation(Math.round(range.low)) : '';
    const expectedHighText = range?.high != null ? formatNumberWithoutAbbreviation(Math.round(range.high)) : '';

    const expectedLowWithDecimals =
        range?.low != null ? formatNumberWithoutAbbreviation(_.round(range.low, NUMBER_DISPLAY_PRECISION)) : '';
    const expectedHighWithDecimals =
        range?.high != null ? formatNumberWithoutAbbreviation(_.round(range.high, NUMBER_DISPLAY_PRECISION)) : '';

    switch (volumeAssertion?.type) {
        case VolumeAssertionType.RowCountChange:
            return undefined; // Not supported yet
        case VolumeAssertionType.RowCountTotal:
            return {
                actualText: formattedActual,
                expectedLowText,
                expectedHighText,
                expectedLowTextWithDecimals: expectedLowWithDecimals,
                expectedHighTextWithDecimals: expectedHighWithDecimals,
            };
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
export const getResultStatusText = (
    result: AssertionResultType,
    type: ResultStatusType,
    isSmartAssertion?: boolean,
) => {
    const initText = type === ResultStatusType.LATEST ? 'Initializing' : 'Initialized';
    switch (result) {
        case AssertionResultType.Success:
            return type === ResultStatusType.LATEST ? 'Passing' : 'Passed';
        case AssertionResultType.Failure:
            return type === ResultStatusType.LATEST ? 'Failing' : 'Failed';
        case AssertionResultType.Error:
            return 'Error';
        case AssertionResultType.Init:
            return isSmartAssertion ? 'Training' : initText;
        default:
            throw new Error(`Unsupported Assertion Result Type ${result} provided.`);
    }
};
