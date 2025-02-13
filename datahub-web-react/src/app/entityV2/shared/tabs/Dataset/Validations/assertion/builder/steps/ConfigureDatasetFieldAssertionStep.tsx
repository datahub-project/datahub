import React from 'react';
import styled from 'styled-components';
import { Button, Tooltip } from '@components';
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
import { useConnectionWithRunAssertionCapabilitiesForEntityExists } from '../../../acrylUtils';
import { AssertionActionsSection } from './actions/AssertionActionsSection';

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
    const isTestAssertionActionDisabled = !useConnectionWithRunAssertionCapabilitiesForEntityExists(
        state.entityUrn ?? '',
    );

    return (
        <Step>
            <div>
                <FieldAssertionBuilder state={state} updateState={updateState} disabled={false} />
                <AssertionActionsSection state={state} updateState={updateState} />
            </div>
            <Controls>
                <Button onClick={prev} variant="outline" color="gray">
                    Back
                </Button>
                <ControlsGroup>
                    <Tooltip
                        title={
                            isTestAssertionActionDisabled
                                ? 'Trying assertions is not supported for sources with remote executors.'
                                : 'Try this assertion out!'
                        }
                    >
                        <Button
                            variant="outline"
                            onClick={handleTestAssertionSubmit}
                            disabled={isTestAssertionActionDisabled}
                        >
                            Try it out
                        </Button>
                    </Tooltip>
                    <Button onClick={() => goTo(AssertionBuilderStep.FINISH_UP)}>Next</Button>
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
