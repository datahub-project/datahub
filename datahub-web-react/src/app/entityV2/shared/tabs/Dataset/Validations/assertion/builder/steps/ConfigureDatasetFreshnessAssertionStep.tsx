import React from 'react';
import styled from 'styled-components';
import { Button } from 'antd';
import { Tooltip } from '@components';
import { AssertionBuilderStep, StepProps } from '../types';
import {
    AssertionEvaluationParametersInput,
    AssertionType,
    CreateFreshnessAssertionInput,
} from '../../../../../../../../../types.generated';
import { TestAssertionModal } from './preview/TestAssertionModal';
import { builderStateToTestFreshnessAssertionVariables } from '../utils';
import { useTestAssertionModal } from './utils';
import { DatasetFreshnessAssertionBuilder } from './freshness/DatasetFreshnessAssertionBuilder';
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
 * Step for defining the Dataset Freshness assertion
 */
export const ConfigureDatasetFreshnessAssertionStep = ({ state, updateState, goTo, prev }: StepProps) => {
    const { isTestAssertionModalVisible, handleTestAssertionSubmit, hideTestAssertionModal } = useTestAssertionModal();
    const isTestAssertionActionDisabled = !useConnectionWithRunAssertionCapabilitiesForEntityExists(
        state.entityUrn ?? '',
    );

    return (
        <Step>
            <div>
                <DatasetFreshnessAssertionBuilder state={state} updateState={updateState} disabled={false} />
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
                    type: AssertionType.Freshness,
                    connectionUrn: state.platformUrn as string,
                    freshnessTestInput: builderStateToTestFreshnessAssertionVariables(state)
                        .input as CreateFreshnessAssertionInput,
                    parameters: state.parameters as AssertionEvaluationParametersInput,
                }}
            />
        </Step>
    );
};
