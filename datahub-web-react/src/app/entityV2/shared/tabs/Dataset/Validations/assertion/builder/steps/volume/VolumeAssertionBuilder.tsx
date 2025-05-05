import { Collapse } from 'antd';
import React from 'react';
import styled from 'styled-components';

import {
    AI_INFERRED_ASSERTION_DEFAULT_SCHEDULE_CRON,
    AI_INFERRED_ASSERTION_DEFAULT_SCHEDULE_TIMEZONE,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/constants';
import { EvaluationScheduleBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/common/EvaluationScheduleBuilder';
import {
    VolumeInferenceAdjuster,
    VolumeInferenceAdjusterHandle,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/VolumeInferenceAdjuster';
import { VolumeFilterBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/volume/VolumeFilterBuilder';
import { VolumeParametersBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/volume/VolumeParametersBuilder';
import { VolumeSourceTypeBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/volume/VolumeSourceTypeBuilder';
import { VolumeTypeBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/volume/VolumeTypeBuilder';
import {
    AssertionMonitorBuilderState,
    VolumeAssertionBuilderState,
    VolumeAssertionBuilderTypeOptions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';
import QualityTabRowCountGraph from '@app/entityV2/shared/tabs/Dataset/Validations/shared/QualityTabRowCountGraph';

import { AssertionType, CronSchedule, DatasetVolumeSourceType } from '@types';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 20px;
`;

type Props = {
    state: AssertionMonitorBuilderState;
    updateState: (newState: AssertionMonitorBuilderState) => void;
    disabled?: boolean;
    isEditMode?: boolean;
    onSave?: () => void;
    inferenceAdjusterRef?: React.Ref<VolumeInferenceAdjusterHandle>;
};

export const VolumeAssertionBuilder = ({
    state,
    updateState,
    disabled,
    isEditMode,
    onSave,
    inferenceAdjusterRef,
}: Props) => {
    const assertion = state?.assertion;
    const schedule: CronSchedule | undefined | null = state?.schedule;
    const volumeAssertion = assertion?.volumeAssertion;
    const volumeParameters = volumeAssertion?.parameters;
    const filter = volumeAssertion?.filter;
    const sourceType = state.parameters?.datasetVolumeParameters?.sourceType;
    const entityUrn = state?.entityUrn as string;
    const platformUrn = state?.platformUrn as string;

    const isAiInferenceSelected = volumeAssertion?.type === VolumeAssertionBuilderTypeOptions.AiInferredRowCountTotal;

    const updateAssertionSchedule = (newSchedule: CronSchedule) => {
        updateState({
            ...state,
            schedule: newSchedule,
        });
    };

    const updateVolumeType = (newVolumeAssertion: VolumeAssertionBuilderState) => {
        const isAiInferred = newVolumeAssertion.type === VolumeAssertionBuilderTypeOptions.AiInferredRowCountTotal;
        updateState({
            ...state,
            schedule: isAiInferred
                ? {
                      timezone: state.schedule?.timezone || AI_INFERRED_ASSERTION_DEFAULT_SCHEDULE_TIMEZONE,
                      cron: AI_INFERRED_ASSERTION_DEFAULT_SCHEDULE_CRON,
                  }
                : state.schedule,
            assertion: {
                ...state.assertion,
                volumeAssertion: {
                    ...newVolumeAssertion,
                },
            },
        });
    };

    const updateVolumeParameters = (newVolumeParameters: VolumeAssertionBuilderState['parameters']) => {
        updateState({
            ...state,
            assertion: {
                ...state.assertion,
                volumeAssertion: {
                    ...volumeAssertion,
                    parameters: newVolumeParameters,
                },
            },
        });
    };

    const updateSourceType = (newSourceType: DatasetVolumeSourceType) => {
        updateState({
            ...state,
            assertion: {
                ...state.assertion,
                volumeAssertion: {
                    ...volumeAssertion,
                    filter: undefined, // Reset filter when source type changes
                },
            },
            parameters: {
                ...state.parameters,
                datasetVolumeParameters: {
                    ...state.parameters?.datasetVolumeParameters,
                    sourceType: newSourceType,
                },
            },
        });
    };

    const updateFilter = (newFilter?: VolumeAssertionBuilderState['filter']) => {
        updateState({
            ...state,
            assertion: {
                ...state.assertion,
                volumeAssertion: {
                    ...volumeAssertion,
                    filter: newFilter,
                },
            },
        });
    };

    const updateVolumeAssertion = (newVolumeAssertion: VolumeAssertionBuilderState) => {
        updateState({
            ...state,
            assertion: {
                ...state.assertion,
                volumeAssertion: {
                    ...state.assertion?.volumeAssertion,
                    ...newVolumeAssertion,
                },
            },
        });
    };

    return (
        <div>
            {/* Cannot change type for AI inferred volume assertions */}
            {!(isEditMode && isAiInferenceSelected) && (
                <VolumeTypeBuilder
                    volumeInfo={volumeAssertion}
                    onChange={updateVolumeType}
                    disabled={disabled}
                    isEditMode={isEditMode}
                />
            )}
            <QualityTabRowCountGraph />
            <br />
            {/* hidden for ai inferred assertions */}
            {!isAiInferenceSelected && (
                <VolumeParametersBuilder
                    volumeInfo={volumeAssertion}
                    value={volumeParameters}
                    onChange={updateVolumeParameters}
                    updateVolumeAssertion={updateVolumeAssertion}
                    disabled={disabled}
                />
            )}
            <Section>
                <Collapse>
                    <Collapse.Panel key="Advanced" header="Volume collection mechanism">
                        <VolumeSourceTypeBuilder
                            entityUrn={entityUrn}
                            platformUrn={platformUrn}
                            value={sourceType as DatasetVolumeSourceType}
                            onChange={updateSourceType}
                            disabled={disabled}
                        />
                        <VolumeFilterBuilder
                            value={filter}
                            onChange={updateFilter}
                            sourceType={sourceType as DatasetVolumeSourceType}
                            disabled={disabled}
                        />
                    </Collapse.Panel>
                </Collapse>
            </Section>
            {isAiInferenceSelected ? (
                <VolumeInferenceAdjuster
                    ref={inferenceAdjusterRef}
                    state={state}
                    updateState={updateState}
                    disabled={disabled}
                    onSave={onSave}
                />
            ) : (
                <EvaluationScheduleBuilder
                    value={schedule}
                    assertionType={AssertionType.Volume}
                    onChange={updateAssertionSchedule}
                    disabled={disabled}
                />
            )}
        </div>
    );
};
