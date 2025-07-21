import { useState } from 'react';

import { getFieldMetricTypeReadableLabel } from '@app/entity/shared/tabs/Dataset/Validations/fieldDescriptionUtils';

import { useUpsertDatasetFieldAssertionMonitorMutation } from '@graphql/assertion.generated';
import {
    AssertionActionsInput,
    AssertionAdjustmentSettingsInput,
    AssertionStdOperator,
    AssertionStdParameterType,
    CronScheduleInput,
    DatasetFieldAssertionParametersInput,
    FieldAssertionType,
    FieldMetricType,
    MonitorMode,
    SchemaFieldSpecInput,
    UpsertDatasetFieldAssertionMonitorInput,
} from '@types';

export type BulkFieldAssertionSpec = {
    entityUrn: string;
    fields: {
        field: SchemaFieldSpecInput;
        metrics: FieldMetricType[];
    }[];
    evaluationParameters: DatasetFieldAssertionParametersInput;
    actions?: AssertionActionsInput;
    inferenceSettings?: AssertionAdjustmentSettingsInput;
    evaluationSchedule: CronScheduleInput;
};

export type ProgressReport = {
    total: number;
    completed: number;
    successful: {
        field: SchemaFieldSpecInput | 'Unknown';
        metric: FieldMetricType | 'Unknown';
    }[];
    errored: {
        field: SchemaFieldSpecInput | 'Unknown';
        metric: FieldMetricType | 'Unknown';
        error: string;
    }[];
};

const DEFAULT_PROGRESS_REPORT: ProgressReport = {
    total: 0,
    completed: 0,
    successful: [],
    errored: [],
};

/**
 * Upserts the bulk field assertions for the given assertion spec.
 * Tracks the progress of the upsert operation in the progress report.
 * Captures the successful and errored assertions in the progress report, along with the error messages.
 */
export const useUpsertBulkFieldAssertions = () => {
    const [upsertFieldAssertionMonitorMutation] = useUpsertDatasetFieldAssertionMonitorMutation();

    const [progressReport, setProgressReport] = useState<ProgressReport>(DEFAULT_PROGRESS_REPORT);

    const upsertBulkFieldAssertions = (assertionSpec: BulkFieldAssertionSpec) => {
        // 1. Build the upsert inputs
        const upsertInputs: UpsertDatasetFieldAssertionMonitorInput[] = [];
        assertionSpec.fields.forEach((field) => {
            field.metrics.forEach((metric) => {
                upsertInputs.push({
                    // hardcoded
                    inferWithAI: true,
                    mode: MonitorMode.Active,
                    type: FieldAssertionType.FieldMetric,
                    // params
                    entityUrn: assertionSpec.entityUrn,
                    description: `Anomaly monitor for ${getFieldMetricTypeReadableLabel(metric)} on ${field.field.path}`,
                    evaluationParameters: assertionSpec.evaluationParameters,
                    evaluationSchedule: assertionSpec.evaluationSchedule,
                    actions: assertionSpec.actions,
                    inferenceSettings: assertionSpec.inferenceSettings,
                    fieldMetricAssertion: {
                        field: field.field,
                        metric,
                        operator: AssertionStdOperator.Between,
                        // param values do not matter since will be inferred
                        parameters: {
                            maxValue: {
                                value: '10',
                                type: AssertionStdParameterType.Number,
                            },
                            minValue: {
                                value: '0',
                                type: AssertionStdParameterType.Number,
                            },
                        },
                    },
                });
            });
        });

        // 2. Initialize the progress report
        setProgressReport({
            ...DEFAULT_PROGRESS_REPORT,
            total: upsertInputs.length,
        });

        // 3. Iterate through the upsert inputs and upsert each one
        upsertInputs.forEach((upsertInput) => {
            upsertFieldAssertionMonitorMutation({
                variables: {
                    input: upsertInput,
                },
            })
                .then((result) => {
                    if (result.data?.upsertDatasetFieldAssertionMonitor) {
                        setProgressReport((currentReport) => ({
                            ...currentReport,
                            completed: currentReport.completed + 1,
                            successful: [
                                ...currentReport.successful,
                                {
                                    field: upsertInput.fieldMetricAssertion?.field || 'Unknown',
                                    metric: upsertInput.fieldMetricAssertion?.metric || 'Unknown',
                                },
                            ],
                        }));
                    }
                })
                .catch((error) => {
                    setProgressReport((currentReport) => ({
                        ...currentReport,
                        completed: currentReport.completed + 1,
                        errored: [
                            ...currentReport.errored,
                            {
                                field: upsertInput.fieldMetricAssertion?.field || 'Unknown',
                                metric: upsertInput.fieldMetricAssertion?.metric || 'Unknown',
                                error: error.message,
                            },
                        ],
                    }));
                });
        });

        // 4. Return the progress report
        return progressReport;
    };

    return {
        upsertBulkFieldAssertions,
        progressReport,
    };
};
