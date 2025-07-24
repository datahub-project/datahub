import { SimpleSelect, Text, ToggleCard } from '@components';
import { Divider, Form, InputNumber, Radio, message } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { AssertionActionsForm } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/AssertionActionsForm';
import { EvaluationScheduleBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/common/EvaluationScheduleBuilder';
import { FreshnessInfrenceAdjuster } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/FreshnessInfrenceAdjuster';
import {
    AssertionMonitorBuilderState,
    FreshnessAssertionBuilderScheduleType,
    FreshnessAssertionScheduleBuilderTypeOptions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';
import {
    getDefaultFreshnessSourceOption,
    getFreshnessSourceOptions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/utils';
import { INVALID_FRESHNESS_SOURCE_TYPES } from '@app/observe/shared/bulkCreate/constants';
import {
    DEFAULT_ASSERTION_ACTIONS,
    DEFAULT_INFERENCE_SETTINGS,
} from '@app/observe/shared/bulkCreate/form/BulkCreateAssertionsForm.constants';
import { FreshnessFormState } from '@app/observe/shared/bulkCreate/form/types';

import { AssertionActionsInput, AssertionType, CronSchedule, DatasetFreshnessSourceType, DateInterval } from '@types';

const RadioGroup = styled(Radio.Group)({
    display: 'flex',
    flexDirection: 'column',
    gap: 10,
});

const FreshnessIntervalLabelWrapper = styled.div({
    display: 'flex',
    alignItems: 'center',
    gap: 10,
});

export const useFreshnessForm = ({
    selectedPlatformUrn,
    canEnableAssertions,
}: {
    selectedPlatformUrn?: string;
    canEnableAssertions: boolean;
}): {
    component: React.ReactNode;
    state: FreshnessFormState;
} => {
    // --------------------------------- State variables --------------------------------- //
    const [freshnessAssertionEnabled, setFreshnessAssertionEnabled] = useState<boolean>(false);
    const [freshnessAssertionType, setFreshnessAssertionType] = useState<FreshnessAssertionBuilderScheduleType>(
        FreshnessAssertionScheduleBuilderTypeOptions.AiInferred,
    );
    const [freshnessAssertionInterval, setFreshnessAssertionInterval] = useState<number>(6);
    const [freshnessAssertionIntervalUnit, setFreshnessAssertionIntervalUnit] = useState<DateInterval>(
        DateInterval.Hour,
    );

    const [freshnessInferenceSettings, setFreshnessInferenceSettings] =
        useState<AssertionMonitorBuilderState['inferenceSettings']>(DEFAULT_INFERENCE_SETTINGS);

    const [freshnessSchedule, setFreshnessSchedule] = useState<CronSchedule>();

    const [freshnessActions, setFreshnessActions] = useState<AssertionActionsInput>(DEFAULT_ASSERTION_ACTIONS);

    const freshnessSourceOptions = selectedPlatformUrn
        ? getFreshnessSourceOptions(selectedPlatformUrn, true).filter(
              (source) => !INVALID_FRESHNESS_SOURCE_TYPES.includes(source.type),
          )
        : [];
    const defaultFreshnessSource = selectedPlatformUrn
        ? getDefaultFreshnessSourceOption(selectedPlatformUrn, true)
        : undefined;
    const [selectedFreshnessSource, setSelectedFreshnessSource] = useState<DatasetFreshnessSourceType | undefined>(
        defaultFreshnessSource,
    );
    useEffect(() => {
        if (defaultFreshnessSource && !selectedFreshnessSource) {
            setSelectedFreshnessSource(defaultFreshnessSource);
        }
    }, [defaultFreshnessSource, selectedFreshnessSource]);

    // --------------------------------- Render UI --------------------------------- //
    const component = (
        <ToggleCard
            title="Freshness"
            value={freshnessAssertionEnabled}
            onToggle={() => {
                if (canEnableAssertions) {
                    setFreshnessAssertionEnabled((enabled) => !enabled);
                } else {
                    message.warn('Please select a platform to enable freshness assertions.');
                }
            }}
        >
            {/* --------------------------------- Title --------------------------------- */}
            <Text size="md" color="gray" colorLevel={1700}>
                Monitor the freshness of datasets.
            </Text>

            {/* --------------------------------- Form --------------------------------- */}
            {freshnessAssertionEnabled && [
                // --------------------------------- Freshness Assertion Type --------------------------------- //
                <RadioGroup value={freshnessAssertionType} onChange={(e) => setFreshnessAssertionType(e.target.value)}>
                    {/* ********** AI Inferred ********** */}
                    <Radio value={FreshnessAssertionScheduleBuilderTypeOptions.AiInferred}>
                        <Text size="md" color="gray" colorLevel={600} weight="medium">
                            Detect freshness anomalies with AI
                        </Text>
                        <Text size="sm" color="gray" colorLevel={1700}>
                            Use AI to continuously monitor for stale data.
                        </Text>
                    </Radio>
                    {/* ********** Since the last check ********** */}
                    <Radio value={FreshnessAssertionScheduleBuilderTypeOptions.SinceTheLastCheck}>
                        <Text size="md" color="gray" colorLevel={600} weight="medium">
                            Since the last check
                        </Text>
                        <Text size="sm" color="gray" colorLevel={1700}>
                            Ensure datasets are updated in between checks, run on a configurable schedule.
                        </Text>
                    </Radio>
                    {/* ********** Fixed Interval ********** */}
                    <Radio value={FreshnessAssertionScheduleBuilderTypeOptions.FixedInterval}>
                        <FreshnessIntervalLabelWrapper>
                            <Text size="md" color="gray" colorLevel={600} weight="medium">
                                In the past
                            </Text>
                            {/* ********** Interval Value ********** */}
                            <InputNumber
                                min={1}
                                value={freshnessAssertionInterval}
                                onChange={(value) => {
                                    if (typeof value === 'number') {
                                        setFreshnessAssertionInterval(Math.round(value));
                                    }
                                }}
                            />
                            {/* ********** Interval Unit ********** */}
                            <SimpleSelect
                                options={[
                                    {
                                        label: 'Minutes',
                                        value: DateInterval.Minute,
                                    },
                                    {
                                        label: 'Hours',
                                        value: DateInterval.Hour,
                                    },
                                    {
                                        label: 'Days',
                                        value: DateInterval.Day,
                                    },
                                ]}
                                values={[freshnessAssertionIntervalUnit]}
                                isMultiSelect={false}
                                showClear={false}
                                width="fit-content"
                                onUpdate={(values) => {
                                    if (values.length > 0) {
                                        setFreshnessAssertionIntervalUnit(values[0].toUpperCase() as DateInterval);
                                    }
                                }}
                            />
                        </FreshnessIntervalLabelWrapper>
                        <Text size="sm" color="gray" colorLevel={1700}>
                            Check for a table update within a certain period of time, run on a configurable schedule.
                        </Text>
                    </Radio>
                </RadioGroup>,
                // --------------------------------- Freshness Inference Settings --------------------------------- //
                // We have to wrap this in a Form since the nested components depend on antd Form context.
                <Form>
                    {freshnessAssertionType === FreshnessAssertionScheduleBuilderTypeOptions.AiInferred ? (
                        <FreshnessInfrenceAdjuster
                            state={{ inferenceSettings: freshnessInferenceSettings }}
                            updateState={(newState) => setFreshnessInferenceSettings(newState.inferenceSettings)}
                            collapsable
                        />
                    ) : (
                        <EvaluationScheduleBuilder
                            value={freshnessSchedule}
                            onChange={(newSchedule) => setFreshnessSchedule(newSchedule)}
                            assertionType={AssertionType.Freshness}
                        />
                    )}
                </Form>,
                // --------------------------------- Freshness Detection Mechanism --------------------------------- //
                <SimpleSelect
                    label="Freshness Detection Mechanism"
                    options={freshnessSourceOptions.map((source) => ({
                        label: source.name,
                        value: source.type,
                        description: source.description,
                    }))}
                    values={selectedFreshnessSource ? [selectedFreshnessSource] : []}
                    isMultiSelect={false}
                    showClear={false}
                    width="fit-content"
                    position="start"
                    descriptionMaxWidth={600}
                    onUpdate={(values) => {
                        if (values.length > 0) {
                            setSelectedFreshnessSource(values[0]?.toUpperCase() as DatasetFreshnessSourceType);
                        }
                    }}
                />,
                <Divider style={{ margin: '8px 0' }} />,
                // --------------------------------- Assertion Actions --------------------------------- //
                <AssertionActionsForm
                    state={freshnessActions}
                    updateState={(newState) =>
                        setFreshnessActions({
                            onFailure: newState.onFailure ?? [],
                            onSuccess: newState.onSuccess ?? [],
                        })
                    }
                />,
            ]}
        </ToggleCard>
    );

    return {
        component,
        state: {
            freshnessAssertionEnabled,
            freshnessAssertionType,
            freshnessAssertionInterval,
            freshnessAssertionIntervalUnit,
            freshnessInferenceSettings,
            freshnessSchedule,
            freshnessActions,
            freshnessSourceType: selectedFreshnessSource ?? DatasetFreshnessSourceType.DatahubOperation,
        },
    };
};
