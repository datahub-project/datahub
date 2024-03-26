import React from 'react';
import styled from 'styled-components';
import { Button } from 'antd';
import { AssertionBuilderStep, StepProps } from '../types';
import {
    AssertionEvaluationParametersInput,
    AssertionType,
    CreateFieldAssertionInput,
} from '../../../../../../../../../types.generated';
import { TestAssertionModal } from './preview/TestAssertionModal';
import { builderStateToTestFieldAssertionVariables } from '../utils';
import { useTestAssertionModal } from './utils';
import { FieldAssertionBuilder } from './field/FieldAssertionBuilder';

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
    margin-bottom: 20px;
`;

const ControlsGroup = styled.div`
    display: flex;
    gap: 8px;
`;

/**
 * Step for defining the Dataset Field assertion
 */
export const ConfigureDatasetFieldAssertionStep = ({ state, updateState, goTo, prev }: StepProps) => {
    const { isTestAssertionModalVisible, handleTestAssertionSubmit, hideTestAssertionModal } = useTestAssertionModal();

    return (
        <Step>
            <FieldAssertionBuilder state={state} updateState={updateState} disabled={false} />
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
                    type: AssertionType.Field,
                    connectionUrn: state.platformUrn as string,
                    fieldTestInput: builderStateToTestFieldAssertionVariables(state).input as CreateFieldAssertionInput,
                    parameters: state.parameters as AssertionEvaluationParametersInput,
                }}
            />
        </Step>
    );
};
