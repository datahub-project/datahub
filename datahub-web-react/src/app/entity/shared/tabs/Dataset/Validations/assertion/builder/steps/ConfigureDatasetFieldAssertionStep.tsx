import React, { useState } from 'react';
import styled from 'styled-components';
import { Button, Collapse } from 'antd';
import useFormInstance from 'antd/lib/form/hooks/useFormInstance';
import { AssertionBuilderStep, StepProps } from '../types';
import { EvaluationScheduleBuilder } from './freshness/EvaluationScheduleBuilder';
import {
    AssertionEvaluationParametersInput,
    AssertionType,
    CreateFieldAssertionInput,
    CronSchedule,
    DatasetFieldAssertionSourceType,
    DatasetFilter,
    FieldAssertionType,
} from '../../../../../../../../../types.generated';
import { TestAssertionModal } from './preview/TestAssertionModal';
import { builderStateToCreateFieldAssertionVariables } from '../utils';
import { FieldColumnBuilder } from './field/FieldColumnBuilder';
import { FieldTypeBuilder } from './field/FieldTypeBuilder';
import { FieldFilterBuilder } from './field/FieldFilterBuilder';
import { FieldErrorThresholdBuilder } from './field/FieldErrorThresholdBuilder';
import { FieldSourceBuilder } from './field/FieldSourceBuilder';
import { FieldValuesParameterBuilder } from './field/FieldValuesParameterBuilder';
import { FieldNullCheckBuilder } from './field/FieldNullCheckBuilder';

const Step = styled.div`
    height: 100%;
    display: flex;
    flex-direction: column;
    justify-content: space-between;
`;

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 20px;
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
 * Step for defining the Dataset Field assertion
 */
export const ConfigureDatasetFieldAssertionStep = ({ state, updateState, goTo, prev }: StepProps) => {
    const [testAssertionModalVisible, setTestAssertionModalVisible] = useState(false);
    const fieldAssertion = state.assertion?.fieldAssertion;
    const parameters = state.parameters?.datasetFieldParameters;
    const form = useFormInstance();

    const updateAssertionSchedule = (schedule: CronSchedule) => {
        updateState({
            ...state,
            schedule,
        });
    };

    const updateFilter = (newFilter?: DatasetFilter) => {
        updateState({
            ...state,
            assertion: {
                ...state.assertion,
                fieldAssertion: {
                    ...state.assertion?.fieldAssertion,
                    filter: newFilter,
                },
            },
        });
    };

    const handleTestAssertion = async () => {
        try {
            await form.validateFields();
            setTestAssertionModalVisible(true);
        } catch {
            // Ignore validation errors
        }
    };

    return (
        <Step>
            <div>
                <EvaluationScheduleBuilder
                    value={state.schedule as CronSchedule}
                    onChange={updateAssertionSchedule}
                    assertionType={AssertionType.Field}
                    showAdvanced={false}
                />
                <FieldTypeBuilder value={state} onChange={updateState} />
                <FieldColumnBuilder value={state} onChange={updateState} />
                {fieldAssertion?.type === FieldAssertionType.FieldValues &&
                    fieldAssertion.fieldValuesAssertion?.field?.path && (
                        <FieldValuesParameterBuilder value={state} onChange={updateState} />
                    )}
                <FieldNullCheckBuilder value={state} onChange={updateState} />
                <FieldSourceBuilder value={state} onChange={updateState} />
                <Section>
                    <Collapse>
                        <Collapse.Panel key="Advanced" header="Advanced">
                            <FieldFilterBuilder
                                value={fieldAssertion?.filter as DatasetFilter}
                                onChange={updateFilter}
                                sourceType={parameters?.sourceType as DatasetFieldAssertionSourceType}
                            />
                            <FieldErrorThresholdBuilder value={state} onChange={updateState} />
                        </Collapse.Panel>
                    </Collapse>
                </Section>
            </div>
            <Controls>
                <Button onClick={prev}>Back</Button>
                <ControlsGroup>
                    <Button onClick={handleTestAssertion}>Try it out</Button>
                    <Button type="primary" onClick={() => goTo(AssertionBuilderStep.CONFIGURE_ACTIONS)}>
                        Next
                    </Button>
                </ControlsGroup>
            </Controls>
            <TestAssertionModal
                visible={testAssertionModalVisible}
                handleClose={() => setTestAssertionModalVisible(false)}
                input={{
                    type: AssertionType.Field,
                    connectionUrn: state.platformUrn as string,
                    fieldTestInput: builderStateToCreateFieldAssertionVariables(state)
                        .input as CreateFieldAssertionInput,
                    parameters: state.parameters as AssertionEvaluationParametersInput,
                }}
            />
        </Step>
    );
};
