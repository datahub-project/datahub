import { useRef, useState } from 'react';

import analytics, { EventType } from '@app/analytics';
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
export const useBulkCreateFieldAssertions = () => {
    const [upsertFieldAssertionMonitorMutation] = useUpsertDatasetFieldAssertionMonitorMutation();

    const [progressReport, setProgressReport] = useState<ProgressReport>(DEFAULT_PROGRESS_REPORT);
    const progressReportRef = useRef<ProgressReport>(progressReport);
    progressReportRef.current = progressReport;

    const upsertBulkFieldAssertions = async (assertionSpec: BulkFieldAssertionSpec) => {
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
        try {
            analytics.event({
                type: EventType.BulkCreateAssertionSubmissionEvent,
                surface: 'field-metric-assertion-builder',
                entityCount: upsertInputs.length,
                hasFieldMetricAssertion: true,
                hasFreshnessAssertion: false,
                hasVolumeAssertion: false,
                hasSubscription: false,
            });
        } catch (error) {
            console.error('Error sending bulk create assertion submission event', error);
        }

        // 3. Iterate through the upsert inputs and upsert each one
        await Promise.allSettled(
            upsertInputs.map((upsertInput) =>
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
                    }),
            ),
        );

        // 4. Send the completed event
        const latestProgressReport = progressReportRef.current;
        try {
            analytics.event({
                type: EventType.BulkCreateAssertionCompletedEvent,
                surface: 'field-metric-assertion-builder',
                entityCount: upsertInputs.length,
                failedAssertionCount: latestProgressReport.errored.length,
                successAssertionCount: latestProgressReport.successful.length,
                totalAssertionCount: latestProgressReport.total,
                hasFreshnessAssertion: false,
                hasFieldMetricAssertion: true,
                hasVolumeAssertion: false,
                hasSubscription: false,
                successSubscriptionCount: 0,
                failedSubscriptionCount: 0,
            });
        } catch (error) {
            console.error('Error sending bulk create assertion completed event', error);
        }

        // 5. Return the progress report
        return progressReportRef.current;
    };

    return {
        upsertBulkFieldAssertions,
        progressReport,
    };
};
