import React from 'react';
import styled from 'styled-components';
import { Button, Tooltip } from '@components';
import { AssertionBuilderStep, StepProps, VolumeAssertionBuilderTypeOptions } from '../types';
import {
    AssertionEvaluationParametersInput,
    AssertionType,
    CreateVolumeAssertionInput,
} from '../../../../../../../../../types.generated';
import { TestAssertionModal } from './preview/TestAssertionModal';
import { builderStateToTestVolumeAssertionVariables } from '../utils';
import { useTestAssertionModal } from './utils';
import { VolumeAssertionBuilder } from './volume/VolumeAssertionBuilder';
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
    const isTestAssertionActionDisabled = !useConnectionWithRunAssertionCapabilitiesForEntityExists(
        state.entityUrn ?? '',
    );
    const isAiInferred =
        state.assertion?.volumeAssertion?.type === VolumeAssertionBuilderTypeOptions.AiInferredRowCountTotal;

    return (
        <Step>
            <div>
                <VolumeAssertionBuilder state={state} updateState={updateState} disabled={false} />
                <AssertionActionsSection state={state} updateState={updateState} />
            </div>
            <Controls>
                {prev && (
                    <Button variant="outline" color="gray" onClick={prev}>
                        Back
                    </Button>
                )}
                <ControlsGroup>
                    {!isAiInferred && (
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
                    )}
                    <Button onClick={() => goTo(AssertionBuilderStep.FINISH_UP)}>Next</Button>
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
