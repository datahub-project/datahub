import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { AssertionActionsSection } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/actions/AssertionActionsSection';
import { TestAssertionModal } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/preview/TestAssertionModal';
import { SchemaAssertionBuilder } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/schema/SchemaAssertionBuilder';
import { useTestAssertionModal } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/utils';
import { AssertionBuilderStep, StepProps } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/types';
import { builderStateToTestSchemaAssertionVariables } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/utils';

import { AssertionEvaluationParametersInput, AssertionType, CreateSchemaAssertionInput } from '@types';

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
 * Step for defining the Dataset Schema assertion
 * TODO: Add support for trying this type of assertion out.
 */
export const ConfigureDatasetSchemaAssertionStep = ({ state, updateState, goTo, prev }: StepProps) => {
    const { isTestAssertionModalVisible, handleTestAssertionSubmit, hideTestAssertionModal } = useTestAssertionModal();
    return (
        <Step>
            <div>
                <SchemaAssertionBuilder state={state} updateState={updateState} disabled={false} />
                <AssertionActionsSection state={state} updateState={updateState} />
            </div>
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
                    type: AssertionType.DataSchema,
                    connectionUrn: state.platformUrn as string,
                    schemaTestInput: builderStateToTestSchemaAssertionVariables(state) as CreateSchemaAssertionInput,
                    parameters: state.parameters as AssertionEvaluationParametersInput,
                }}
            />
        </Step>
    );
};
