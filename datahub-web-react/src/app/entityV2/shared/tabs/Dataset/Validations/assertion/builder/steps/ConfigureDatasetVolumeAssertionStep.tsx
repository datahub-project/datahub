import React from 'react';
import styled from 'styled-components';
import { Button } from 'antd';
import { AssertionBuilderStep, StepProps } from '../types';
import {
    AssertionEvaluationParametersInput,
    AssertionType,
    CreateVolumeAssertionInput,
} from '../../../../../../../../../types.generated';
import { TestAssertionModal } from './preview/TestAssertionModal';
import { builderStateToTestVolumeAssertionVariables } from '../utils';
import { useTestAssertionModal } from './utils';
import { VolumeAssertionBuilder } from './volume/VolumeAssertionBuilder';

const Step = styled.div`
    height: 100%;
    display: flex;
    flex-direction: column;
    justify-content: space-between;
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
    const { isTestAssertionModalVisible, handleTestAssertionSubmit, hideTestAssertionModal } = useTestAssertionModal();
    return (
        <Step>
            <VolumeAssertionBuilder state={state} updateState={updateState} editing />
            <Controls>
                <Button onClick={prev}>Back</Button>
                <ControlsGroup>
                    <Button onClick={handleTestAssertionSubmit}>Try it out</Button>
                    <Button type="primary" onClick={() => goTo(AssertionBuilderStep.FINISH_UP)}>
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
                    volumeTestInput: builderStateToTestVolumeAssertionVariables(state)
                        .input as CreateVolumeAssertionInput,
                    parameters: state.parameters as AssertionEvaluationParametersInput,
                }}
            />
        </Step>
    );
};
