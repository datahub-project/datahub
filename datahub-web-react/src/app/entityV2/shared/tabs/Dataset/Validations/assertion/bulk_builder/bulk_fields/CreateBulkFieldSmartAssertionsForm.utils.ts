import { uniq, uniqBy } from 'lodash';

import { getEligibleChangedRowColumns } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/field/utils';
import {
    EligibleFieldColumn,
    getFieldMetricTypeOptions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/field/utils';
import { DEFAULT_SMART_ASSERTION_SENSITIVITY } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/InferenceSensitivityAdjuster';
import { DEFAULT_SMART_ASSERTION_TRAINING_LOOKBACK_WINDOW_DAYS } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/LookBackWindowAdjuster';
import { AssertionMonitorBuilderState } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';
import { BulkFieldAssertionSpec } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/bulk_builder/bulk_fields/useBulkCreateFieldAssertions';

import { useGetDatasetSchemaQuery } from '@graphql/dataset.generated';
import {
    AssertionActionsInput,
    CronSchedule,
    DatasetFieldAssertionSourceType,
    DayOfWeek,
    FieldMetricType,
    FreshnessFieldSpecInput,
    SchemaField,
} from '@types';

type AvailableMetricForColumns = {
    columnName: string;
    availableMetrics: {
        value: FieldMetricType;
        label: string;
        requiresConnection?: boolean;
    }[];
};

/**
 * Builds the selected columns and metrics from the updated values of the select component
 * @param values - The values to build the selected columns and metrics from
 * @param columns - The columns to build the selected columns and metrics from
 * @param selectedColumnsAndMetrics - The currently selected columns and metrics
 * @returns The selected columns and metrics
 */
export const buildSelectedColumnsAndMetricsOnColumnsUpdate = (
    values: string[],
    columns: EligibleFieldColumn[],
    selectedColumnsAndMetrics: SelectedColumnsAndMetrics[],
): SelectedColumnsAndMetrics[] => {
    return uniqBy(
        columns.filter((column) => values.includes(column.path)),
        'path',
    ).map((column) => ({
        column,
        // Get the existing metrics for the column
        metrics: selectedColumnsAndMetrics.find((c) => c.column.path === column.path)?.metrics || [],
    }));
};

/**
 * Builds the selected columns and metrics from the updated values of the select component
 * This is used when the user updates the metrics for a column
 * @param values - The values to build the selected columns and metrics from
 * @param availableMetricsForColumns - The available metrics for the columns
 * @param selectedColumnsAndMetrics - The currently selected columns and metrics
 * @returns The selected columns and metrics
 */
export const buildSelectedColumnsAndMetricsOnMetricsUpdate = (
    values: string[],
    availableMetricsForColumns: AvailableMetricForColumns[],
    selectedColumnsAndMetrics: SelectedColumnsAndMetrics[],
): SelectedColumnsAndMetrics[] => {
    return selectedColumnsAndMetrics.map((column) => {
        // 1. Get the valid metrics for the column
        const validMetricsForColumn = availableMetricsForColumns.find(
            (c) => c.columnName === column.column.path,
        )?.availableMetrics;
        // 2. Filter the values to only include valid metrics for this column
        const applicableValues = values
            .filter((value) => validMetricsForColumn?.some((m) => m.value === value))
            .map((value) => value as FieldMetricType);
        // 3. Return the column with the new metrics
        return {
            ...column,
            metrics: uniq(applicableValues),
        };
    });
};

/**
 * Returns the valid changed rows fields for the given entity
 * These can be used to figure out new rows that have been added to the dataset
 */
export const useGetValidChangedRowsFields = (entityUrn: string) => {
    const { data: datasetSchema } = useGetDatasetSchemaQuery({
        variables: {
            urn: entityUrn,
        },
        fetchPolicy: 'cache-first',
    });
    const changedRowColumnOptions = datasetSchema?.dataset?.schemaMetadata?.fields
        ? getEligibleChangedRowColumns(datasetSchema.dataset.schemaMetadata.fields as SchemaField[])
        : [];
    const defaultChangedRowsField = changedRowColumnOptions.length > 0 ? changedRowColumnOptions[0] : undefined;

    return {
        changedRowColumnOptions,
        defaultChangedRowsField,
    };
};

/**
 * A column and the metrics that are available for it
 */
export type SelectedColumnsAndMetrics = {
    column: EligibleFieldColumn;
    metrics: FieldMetricType[];
};

/**
 * Returns the metrics that are available for the given selected columns
 * Creates two groups:
 * 1. Valid columns for each metric
 * 2. Metrics that are available for each column
 */
export const getColumnAndMetricOptions = (
    selectedColumnsAndMetrics: SelectedColumnsAndMetrics[],
    sourceType: DatasetFieldAssertionSourceType,
) => {
    const availableMetricsForColumns: AvailableMetricForColumns[] = selectedColumnsAndMetrics.map((column) => ({
        columnName: column.column.path,
        availableMetrics: getFieldMetricTypeOptions(column.column.type, sourceType),
    }));
    const metricOptions = uniqBy(availableMetricsForColumns.map((column) => column.availableMetrics).flat(), 'value');
    // Get the columns that are valid for any given metric
    const validColumnsForEachMetric = metricOptions.map((metric) => ({
        metric,
        columns: availableMetricsForColumns.filter((column) =>
            column.availableMetrics.some((m) => m.value === metric.value),
        ),
    }));

    return {
        metricOptions,
        validColumnsForEachMetric,
        availableMetricsForColumns,
    };
};

/**
 * Creates a BulkFieldAssertionSpec from the state of the builder
 */
export const createBulkFieldAssertionSpecFromState = ({
    entityUrn,
    selectedColumnsAndMetrics,
    schedule,
    sourceType,
    changedRowsField,
    inferenceSettings,
    actions,
}: {
    entityUrn: string;
    selectedColumnsAndMetrics: SelectedColumnsAndMetrics[];
    schedule: CronSchedule;
    sourceType: DatasetFieldAssertionSourceType;
    changedRowsField?: FreshnessFieldSpecInput;
    inferenceSettings: AssertionMonitorBuilderState['inferenceSettings'];
    actions?: AssertionActionsInput;
}): BulkFieldAssertionSpec => {
    return {
        entityUrn,
        fields: selectedColumnsAndMetrics.map((column) => ({
            field: column.column,
            metrics: column.metrics,
        })),
        evaluationSchedule: schedule,
        evaluationParameters: {
            sourceType,
            changedRowsField,
        },
        inferenceSettings: {
            sensitivity: { level: inferenceSettings?.sensitivity?.level ?? DEFAULT_SMART_ASSERTION_SENSITIVITY },
            trainingDataLookbackWindowDays:
                inferenceSettings?.trainingDataLookbackWindowDays ??
                DEFAULT_SMART_ASSERTION_TRAINING_LOOKBACK_WINDOW_DAYS,
            exclusionWindows:
                inferenceSettings?.exclusionWindows?.map((exclusionWindow) => ({
                    type: exclusionWindow.type,
                    displayName: exclusionWindow.displayName,
                    fixedRange: exclusionWindow.fixedRange
                        ? {
                              startTimeMillis: exclusionWindow.fixedRange.startTimeMillis,
                              endTimeMillis: exclusionWindow.fixedRange.endTimeMillis,
                          }
                        : undefined,
                    holiday: exclusionWindow.holiday
                        ? {
                              name: exclusionWindow.holiday.name,
                              region: exclusionWindow.holiday.region,
                              timezone: exclusionWindow.holiday.timezone,
                          }
                        : undefined,
                    weekly: exclusionWindow.weekly
                        ? {
                              daysOfWeek: exclusionWindow.weekly.daysOfWeek?.map((day) => day as DayOfWeek),
                              startTime: exclusionWindow.weekly.startTime,
                              endTime: exclusionWindow.weekly.endTime,
                              timezone: exclusionWindow.weekly.timezone,
                          }
                        : undefined,
                })) ?? [],
        },
        actions,
    };
};
