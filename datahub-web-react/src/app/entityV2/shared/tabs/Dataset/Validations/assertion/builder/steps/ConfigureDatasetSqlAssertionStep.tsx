import React from 'react';
import styled from 'styled-components';
import { Button, Tooltip } from 'antd';
import { AssertionBuilderStep, StepProps } from '../types';
import { AssertionType, CreateSqlAssertionInput } from '../../../../../../../../../types.generated';
import { TestAssertionModal } from './preview/TestAssertionModal';
import { builderStateToTestSqlAssertionVariables } from '../utils';
import { useTestAssertionModal } from './utils';
import { SqlAssertionBuilder } from './sql/SqlAssertionBuilder';
import { useConnectionWithTestAssertionCapabilitiesForEntityExists } from '../../../acrylUtils';

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
    const isTestAssertionActionDisabled = !useConnectionWithTestAssertionCapabilitiesForEntityExists(state.entityUrn ?? '');

    return (
        <Step>
            <SqlAssertionBuilder state={state} updateState={updateState} disabled={false} />
            <Controls>
                <Button onClick={prev}>Back</Button>
                <ControlsGroup>
                    <Tooltip
                        title={isTestAssertionActionDisabled ? 'Trying assertions is not supported for sources with remote executors.' : 'Try this assertion out!'}
                    >
                        <Button onClick={handleTestAssertionSubmit} disabled={isTestAssertionActionDisabled}>Try it out</Button>
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
                    type: AssertionType.Sql,
                    connectionUrn: state.platformUrn as string,
                    sqlTestInput: builderStateToTestSqlAssertionVariables(state).input as CreateSqlAssertionInput,
                }}
            />
        </Step>
    );
};
