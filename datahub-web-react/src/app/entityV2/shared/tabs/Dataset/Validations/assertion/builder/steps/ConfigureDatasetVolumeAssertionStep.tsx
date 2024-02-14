import React from 'react';
import styled from 'styled-components';
import { Button, Collapse } from 'antd';
import { AssertionBuilderStep, StepProps } from '../types';
import {
    AssertionEvaluationParametersInput,
    AssertionStdParameters,
    AssertionType,
    CreateVolumeAssertionInput,
    CronSchedule,
    DatasetFilter,
    DatasetVolumeSourceType,
    IncrementingSegmentSpec,
    VolumeAssertionInfo,
} from '../../../../../../../../../types.generated';
import { EvaluationScheduleBuilder } from './freshness/EvaluationScheduleBuilder';
import { VolumeTypeBuilder } from './volume/VolumeTypeBuilder';
import { VolumeParametersBuilder } from './volume/VolumeParametersBuilder';
import { VolumeSourceTypeBuilder } from './volume/VolumeSourceTypeBuilder';
import { VolumeFilterBuilder } from './volume/VolumeFilterBuilder';
import { TestAssertionModal } from './preview/TestAssertionModal';
import { builderStateToCreateVolumeAssertionVariables } from '../utils';
import { useTestAssertionModal } from './utils';

const Step = styled.div`
    height: 100%;
    display: flex;
    flex-direction: column;
    justify-content: space-between;
`;

const Form = styled.div``;

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 20px;
`;

const Controls = styled.div`
    display: flex;
    justify-content: space-between;
    margin-top: 8px;
`;

const ControlsGroup = styled.div`
    display: flex;
    gap: 8px;
`;

/**
 * Step for defining the Dataset Volume assertion
 */
export const ConfigureDatasetVolumeAssertionStep = ({ state, updateState, goTo, prev }: StepProps) => {
    const volumeAssertion = state.assertion?.volumeAssertion;
    const segment = volumeAssertion?.segment;
    const volumeParameters = volumeAssertion?.parameters;
    const filter = volumeAssertion?.filter;
    const sourceType = state.parameters?.datasetVolumeParameters?.sourceType;
    const { isTestAssertionModalVisible, handleTestAssertionSubmit, hideTestAssertionModal } = useTestAssertionModal();

    const updateAssertionSchedule = (schedule: CronSchedule) => {
        updateState({
            ...state,
            schedule,
        });
    };

    const updateVolumeType = (newVolumeAssertion: Partial<VolumeAssertionInfo>) => {
        updateState({
            ...state,
            assertion: {
                ...state.assertion,
                volumeAssertion: {
                    segment,
                    ...newVolumeAssertion,
                },
            },
        });
    };

    const updateVolumeParameters = (newVolumeParameters: AssertionStdParameters) => {
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

    const updateFilter = (newFilter?: DatasetFilter) => {
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

    const updateVolumeAssertion = (newVolumeAssertion: Partial<VolumeAssertionInfo>) => {
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
        <Step>
            <Form>
                <EvaluationScheduleBuilder
                    value={state.schedule as CronSchedule}
                    onChange={updateAssertionSchedule}
                    assertionType={AssertionType.Volume}
                />
                <VolumeTypeBuilder onChange={updateVolumeType} segment={segment as IncrementingSegmentSpec} />
                <VolumeParametersBuilder
                    volumeInfo={volumeAssertion as VolumeAssertionInfo}
                    value={volumeParameters as AssertionStdParameters}
                    onChange={updateVolumeParameters}
                    updateVolumeAssertion={updateVolumeAssertion}
                />
                <Section>
                    <Collapse>
                        <Collapse.Panel key="Advanced" header="Advanced">
                            <VolumeSourceTypeBuilder
                                entityUrn={state.entityUrn as string}
                                platformUrn={state.platformUrn as string}
                                value={sourceType as DatasetVolumeSourceType}
                                onChange={updateSourceType}
                            />
                            <VolumeFilterBuilder
                                value={filter as DatasetFilter}
                                onChange={updateFilter}
                                sourceType={sourceType as DatasetVolumeSourceType}
                            />
                        </Collapse.Panel>
                    </Collapse>
                </Section>
            </Form>
            <Controls>
                <Button onClick={prev}>Back</Button>
                <ControlsGroup>
                    <Button onClick={handleTestAssertionSubmit}>Try it out</Button>
                    <Button type="primary" onClick={() => goTo(AssertionBuilderStep.CONFIGURE_ACTIONS)}>
                        Next
                    </Button>
                </ControlsGroup>
            </Controls>
            <TestAssertionModal
                visible={isTestAssertionModalVisible}
                handleClose={hideTestAssertionModal}
                input={{
                    type: AssertionType.Volume,
                    connectionUrn: state.platformUrn as string,
                    volumeTestInput: builderStateToCreateVolumeAssertionVariables(state)
                        .input as CreateVolumeAssertionInput,
                    parameters: state.parameters as AssertionEvaluationParametersInput,
                }}
            />
        </Step>
    );
};
