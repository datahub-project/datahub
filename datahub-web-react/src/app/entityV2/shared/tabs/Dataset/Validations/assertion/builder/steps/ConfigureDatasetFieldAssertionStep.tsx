import { Button, Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';

import { useConnectionWithRunAssertionCapabilitiesForEntityExists } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { AssertionActionsSection } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/actions/AssertionActionsSection';
import { FieldAssertionBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/field/FieldAssertionBuilder';
import { TestAssertionModal } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/preview/TestAssertionModal';
import { useTestAssertionModal } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/utils';
import {
    AssertionBuilderStep,
    FieldMetricAssertionBuilderOperatorOptions,
    StepProps,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';
import { builderStateToTestFieldAssertionVariables } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/utils';

import { AssertionEvaluationParametersInput, AssertionType, CreateFieldAssertionInput } from '@types';

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
    const isAiInferred =
        state.assertion?.fieldAssertion?.fieldMetricAssertion?.operator ===
        FieldMetricAssertionBuilderOperatorOptions.AiInferred;

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
                    type: AssertionType.Field,
                    connectionUrn: state.platformUrn as string,
                    fieldTestInput: builderStateToTestFieldAssertionVariables(state).input as CreateFieldAssertionInput,
                    parameters: state.parameters as AssertionEvaluationParametersInput,
                }}
            />
        </Step>
    );
};
