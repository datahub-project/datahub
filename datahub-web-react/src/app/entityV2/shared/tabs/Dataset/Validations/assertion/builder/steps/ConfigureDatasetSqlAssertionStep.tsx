import { Button, Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';

import { useConnectionWithRunAssertionCapabilitiesForEntityExists } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { AssertionActionsSection } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/actions/AssertionActionsSection';
import { TestAssertionModal } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/preview/TestAssertionModal';
import { SqlAssertionBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/sql/SqlAssertionBuilder';
import { useTestAssertionModal } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/utils';
import { AssertionBuilderStep, StepProps } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';
import { builderStateToTestSqlAssertionVariables } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/utils';

import { AssertionType, CreateSqlAssertionInput } from '@types';

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
 * Step for defining the Dataset SQL assertion
 */
export const ConfigureDatasetSqlAssertionStep = ({ state, updateState, goTo, prev }: StepProps) => {
    const { isTestAssertionModalVisible, handleTestAssertionSubmit, hideTestAssertionModal } = useTestAssertionModal();
    const isTestAssertionActionDisabled = !useConnectionWithRunAssertionCapabilitiesForEntityExists(
        state.entityUrn ?? '',
    );

    return (
        <Step>
            <div>
                <SqlAssertionBuilder state={state} updateState={updateState} disabled={false} />
                <AssertionActionsSection state={state} updateState={updateState} />
            </div>
            <Controls>
                {prev && (
                    <Button variant="outline" color="gray" onClick={prev}>
                        Back
                    </Button>
                )}
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
                    type: AssertionType.Sql,
                    connectionUrn: state.platformUrn as string,
                    sqlTestInput: builderStateToTestSqlAssertionVariables(state).input as CreateSqlAssertionInput,
                }}
            />
        </Step>
    );
};
