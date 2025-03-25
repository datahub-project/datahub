import React from 'react';

import { Collapse } from 'antd';
import styled from 'styled-components';

import {
    AssertionMonitorBuilderState,
    VolumeAssertionBuilderState,
    VolumeAssertionBuilderTypeOptions,
} from '../../types';
import { AssertionType, CronSchedule, DatasetVolumeSourceType } from '../../../../../../../../../../types.generated';
import { EvaluationScheduleBuilder } from '../common/EvaluationScheduleBuilder';
import { VolumeTypeBuilder } from './VolumeTypeBuilder';
import { VolumeParametersBuilder } from './VolumeParametersBuilder';
import { VolumeSourceTypeBuilder } from './VolumeSourceTypeBuilder';
import { VolumeFilterBuilder } from './VolumeFilterBuilder';
import {
    AI_INFERRED_ASSERTION_DEFAULT_SCHEDULE_CRON,
    AI_INFERRED_ASSERTION_DEFAULT_SCHEDULE_TIMEZONE,
} from '../../constants';
import { VolumeInferenceAdjuster } from '../inferred/VolumeInferenceAdjuster';

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
};

export const VolumeAssertionBuilder = ({ state, updateState, disabled, isEditMode }: Props) => {
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
                <VolumeInferenceAdjuster state={state} updateState={updateState} disabled={disabled} />
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
