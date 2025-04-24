import { Tooltip } from '@components';
import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { useConnectionWithRunAssertionCapabilitiesForEntityExists } from '@app/entity/shared/tabs/Dataset/Validations/acrylUtils';
import { AssertionActionsSection } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/actions/AssertionActionsSection';
import { TestAssertionModal } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/preview/TestAssertionModal';
import { useTestAssertionModal } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/utils';
import { VolumeAssertionBuilder } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/volume/VolumeAssertionBuilder';
import { AssertionBuilderStep, StepProps } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/types';
import { builderStateToTestVolumeAssertionVariables } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/utils';

import { AssertionEvaluationParametersInput, AssertionType, CreateVolumeAssertionInput } from '@types';

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

    return (
        <Step>
            <div>
                <VolumeAssertionBuilder state={state} updateState={updateState} disabled={false} />
                <AssertionActionsSection state={state} updateState={updateState} />
            </div>
            <Controls>
                <Button onClick={prev}>Back</Button>
                <ControlsGroup>
                    <Tooltip
                        title={
                            isTestAssertionActionDisabled
                                ? 'Trying assertions is not supported for sources with remote executors.'
                                : 'Try this assertion out!'
                        }
                    >
                        <Button onClick={handleTestAssertionSubmit} disabled={isTestAssertionActionDisabled}>
                            Try it out
                        </Button>
                    </Tooltip>
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
