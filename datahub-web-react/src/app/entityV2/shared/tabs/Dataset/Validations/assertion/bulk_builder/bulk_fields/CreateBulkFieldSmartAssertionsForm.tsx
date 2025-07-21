import { Button, Select, SimpleSelect, Tooltip } from '@components';
import { Divider, message } from 'antd';
import React, { useEffect, useState } from 'react';

import { AssertionActionsForm } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/AssertionActionsForm';
import { DEFAULT_ASSERTION_EVALUATION_SCHEDULE } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/constants';
import {
    EligibleFieldColumn,
    getFieldMetricSourceTypeOptions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/field/utils';
import { FieldMetricInferenceAdjuster } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/FieldMetricInferenceAdjuster';
import { DEFAULT_SMART_ASSERTION_SENSITIVITY } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/InferenceSensitivityAdjuster';
import { DEFAULT_SMART_ASSERTION_TRAINING_LOOKBACK_WINDOW_DAYS } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/LookBackWindowAdjuster';
import { AssertionMonitorBuilderState } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';
import {
    SelectedColumnsAndMetrics,
    buildSelectedColumnsAndMetricsOnColumnsUpdate,
    buildSelectedColumnsAndMetricsOnMetricsUpdate,
    createBulkFieldAssertionSpecFromState,
    getColumnAndMetricOptions,
    useGetValidChangedRowsFields,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/bulk_builder/bulk_fields/CreateBulkFieldSmartAssertionsForm.utils';
import { BulkFieldAssertionSpec } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/bulk_builder/bulk_fields/useUpsertBulkFieldAssertions';

import { AssertionActionsInput, CronSchedule, DatasetFieldAssertionSourceType, FreshnessFieldSpecInput } from '@types';

type Props = {
    entityUrn: string;
    columns: EligibleFieldColumn[];
    onSubmit: (assertionSpec: BulkFieldAssertionSpec) => void;
};

export const CreateBulkFieldSmartAssertionsForm = ({ entityUrn, columns, onSubmit }: Props) => {
    // NOTE: if the user gets to this point, we assume that the connection for the entity exists

    // ----- Source Type ----- //
    const sourceTypeOptions = getFieldMetricSourceTypeOptions();
    const [sourceType, setSourceType] = useState<DatasetFieldAssertionSourceType>(
        DatasetFieldAssertionSourceType.AllRowsQuery,
    );

    // ----- Detect changed rows with a given Column ----- //
    const { changedRowColumnOptions, defaultChangedRowsField } = useGetValidChangedRowsFields(entityUrn);
    const [changedRowsField, setChangedRowsField] = useState<FreshnessFieldSpecInput | undefined>(
        defaultChangedRowsField,
    );
    useEffect(() => {
        if (!changedRowsField && defaultChangedRowsField) {
            setChangedRowsField(defaultChangedRowsField);
        }
    }, [defaultChangedRowsField, changedRowsField]);

    // ----- Selected Columns and Metrics ----- //
    const [selectedColumnsAndMetrics, setSelectedColumnsAndMetrics] = useState<SelectedColumnsAndMetrics[]>([]);
    // Get the available metrics for the selected columns (used for the metrics select)
    const { metricOptions, validColumnsForEachMetric, availableMetricsForColumns } = getColumnAndMetricOptions(
        selectedColumnsAndMetrics,
        sourceType,
    );

    // ----- Evaluation Schedule ----- //
    const [schedule, setSchedule] = useState<CronSchedule>({
        cron: DEFAULT_ASSERTION_EVALUATION_SCHEDULE,
        timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
    });

    // ----- Inference Settings ----- //
    const [inferenceSettings, setInferenceSettings] = useState<AssertionMonitorBuilderState['inferenceSettings']>({
        sensitivity: {
            level: DEFAULT_SMART_ASSERTION_SENSITIVITY,
        },
        trainingDataLookbackWindowDays: DEFAULT_SMART_ASSERTION_TRAINING_LOOKBACK_WINDOW_DAYS,
        exclusionWindows: [],
    });

    // ----- Actions ----- //
    const [actions, setActions] = useState<AssertionActionsInput>({
        onFailure: [],
        onSuccess: [],
    });

    // ----- Event Handlers ----- //
    const onCreate = () => {
        onSubmit(
            createBulkFieldAssertionSpecFromState({
                entityUrn,
                selectedColumnsAndMetrics,
                schedule,
                sourceType,
                changedRowsField,
                inferenceSettings,
                actions,
            }),
        );
    };

    const disabled =
        !selectedColumnsAndMetrics.length || selectedColumnsAndMetrics.some((column) => !column.metrics.length);

    return (
        <div>
            {/* ----- Source Type ----- */}
            <SimpleSelect
                options={sourceTypeOptions.map((option) => ({
                    value: option.value,
                    label: option.label,
                    description: option.description,
                }))}
                values={[
                    sourceType === DatasetFieldAssertionSourceType.ChangedRowsQuery
                        ? DatasetFieldAssertionSourceType.AllRowsQuery
                        : sourceType,
                ]}
                onUpdate={(values) => setSourceType(values[0] as DatasetFieldAssertionSourceType)}
                label="Metrics collection mechanism"
                width="fit-content"
                descriptionMaxWidth={640}
                position="start"
                showClear={false}
                isRequired
            />

            {/* ----- Rows to query ----- */}
            {sourceType !== DatasetFieldAssertionSourceType.DatahubDatasetProfile
                ? [
                      <br />,
                      <SimpleSelect
                          options={[
                              {
                                  value: DatasetFieldAssertionSourceType.AllRowsQuery,
                                  label: 'All Rows Query',
                                  description:
                                      'Each time we run the check, we’ll evaluate the condition using all rows in the table. This may not be desirable for large tables.',
                              },
                              {
                                  value: DatasetFieldAssertionSourceType.ChangedRowsQuery,
                                  label: 'Changed Rows Query',
                                  description:
                                      'Each time we run the check, we’ll evaluate the condition using only the rows that have changed since the previous check.',
                              },
                          ]}
                          values={[sourceType]}
                          onUpdate={(values) => {
                              if (
                                  values[0] === DatasetFieldAssertionSourceType.ChangedRowsQuery &&
                                  !changedRowColumnOptions.length
                              ) {
                                  message.error(
                                      'No valid changed rows field found. Please add a changed rows field to the dataset.',
                                  );
                                  return;
                              }
                              setSourceType(values[0] as DatasetFieldAssertionSourceType);
                          }}
                          label="Query selection"
                          width="fit-content"
                          descriptionMaxWidth={640}
                          position="start"
                          showClear={false}
                          isRequired
                      />,
                  ]
                : null}

            {/* ----- Changed Rows Field ----- */}
            {sourceType === DatasetFieldAssertionSourceType.ChangedRowsQuery && changedRowsField
                ? [
                      <br />,
                      <SimpleSelect
                          options={changedRowColumnOptions.map((column) => ({
                              value: column.path,
                              label: column.path,
                          }))}
                          values={[changedRowsField?.path]}
                          onUpdate={(values) =>
                              setChangedRowsField(
                                  changedRowColumnOptions.filter((column) => values.includes(column.path))[0],
                              )
                          }
                          label="Changed Rows Field"
                          showClear={false}
                          isRequired
                          width="fit-content"
                          position="start"
                      />,
                  ]
                : null}

            <Divider />

            {/* ----- Selected Columns ----- */}
            <Select
                showSearch
                showSelectAll
                options={columns.map((column) => ({
                    value: column.path,
                    label: column.path,
                }))}
                values={selectedColumnsAndMetrics.map((column) => column.column.path)}
                onUpdate={(values) => {
                    setSelectedColumnsAndMetrics((state) =>
                        buildSelectedColumnsAndMetricsOnColumnsUpdate(values, columns, state),
                    );
                }}
                onClear={() => setSelectedColumnsAndMetrics([])}
                showClear
                isMultiSelect
                placeholder="Create smart assertions for..."
                label="Select columns"
                width="full"
                isRequired
            />
            <br />

            {/* ----- Metrics ----- */}
            <Select
                options={metricOptions.map((option) => ({
                    value: option.value,
                    label: option.label,
                    description: `Applies to ${validColumnsForEachMetric.find((m) => m.metric.value === option.value)?.columns.length ?? 0}/${selectedColumnsAndMetrics.length} column${selectedColumnsAndMetrics.length > 1 ? 's' : ''}`,
                }))}
                showDescriptions
                values={selectedColumnsAndMetrics.map((column) => column.metrics).flat()}
                onUpdate={(values) => {
                    setSelectedColumnsAndMetrics((state) =>
                        buildSelectedColumnsAndMetricsOnMetricsUpdate(values, availableMetricsForColumns, state),
                    );
                }}
                isMultiSelect
                showSelectAll
                onClear={() =>
                    setSelectedColumnsAndMetrics(
                        selectedColumnsAndMetrics.map((column) => ({
                            ...column,
                            metrics: [],
                        })),
                    )
                }
                width="full"
                position="start"
                showClear
                isRequired
                placeholder="Select metrics..."
                label="Select metrics"
            />

            {/* ----- Inference Settings ----- */}
            <FieldMetricInferenceAdjuster
                state={{ inferenceSettings, schedule }}
                updateState={(newState) => {
                    setInferenceSettings(newState.inferenceSettings);
                    if (newState.schedule) {
                        setSchedule(newState.schedule);
                    }
                }}
                collapsable
            />

            {/* ----- Actions ----- */}
            <AssertionActionsForm
                state={actions}
                updateState={(newState) =>
                    setActions({
                        onFailure: newState.onFailure?.map((action) => ({ type: action.type })) || [],
                        onSuccess: newState.onSuccess?.map((action) => ({ type: action.type })) || [],
                    })
                }
            />

            <Divider />

            {/* ----- Create Assertions ----- */}
            <Tooltip
                title={
                    disabled
                        ? 'Each column must have at least one metric selected.'
                        : 'Create smart assertions for all the selected columns and metrics.'
                }
                placement="top"
            >
                <div style={{ width: 'fit-content' }}>
                    <Button disabled={disabled} onClick={onCreate}>
                        Create Assertions
                    </Button>
                </div>
            </Tooltip>
        </div>
    );
};
