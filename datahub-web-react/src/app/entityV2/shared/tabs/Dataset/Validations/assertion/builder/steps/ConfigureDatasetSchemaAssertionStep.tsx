import React from 'react';
import styled from 'styled-components';
import { Button } from '@src/alchemy-components';
import { AssertionBuilderStep, StepProps } from '../types';
import { AssertionActionsSection } from './actions/AssertionActionsSection';
import { SchemaAssertionBuilder } from './schema/SchemaAssertionBuilder';
import { useTestAssertionModal } from '../../../../../../../../entity/shared/tabs/Dataset/Validations/assertion/builder/steps/utils';
import { TestAssertionModal } from './preview/TestAssertionModal';
import {
    AssertionEvaluationParametersInput,
    AssertionType,
    CreateSchemaAssertionInput,
} from '../../../../../../../../../types.generated';
import { builderStateToTestSchemaAssertionVariables } from '../utils';

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
                {prev && (
                    <Button variant="outline" color="gray" onClick={prev}>
                        Back
                    </Button>
                )}
                <ControlsGroup>
                    <Button variant="outline" onClick={handleTestAssertionSubmit}>
                        Try it out
                    </Button>
                    <Button onClick={() => goTo(AssertionBuilderStep.FINISH_UP)}>Next</Button>
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
