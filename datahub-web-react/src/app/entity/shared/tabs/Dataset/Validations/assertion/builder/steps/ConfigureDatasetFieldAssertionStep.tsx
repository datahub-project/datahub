import React from 'react';
import styled from 'styled-components';
import { Button, Collapse } from 'antd';
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
import { FieldRowCheckBuilder } from './field/FieldRowCheckBuilder';
import { FieldValuesParameterBuilder } from './field/FieldValuesParameterBuilder';
import { FieldNullCheckBuilder } from './field/FieldNullCheckBuilder';
import { FieldMetricBuilder } from './field/FieldMetricBuilder';
import { FieldMetricSourceBuilder } from './field/FieldMetricSourceBuilder';
import { useTestAssertionModal } from './utils';

const Step = styled.div`
    height: 100%;
    display: flex;
    flex-direction: column;
    justify-content: space-between;
`;

const Section = styled.div`
    padding-bottom: 20px;
`;

const AdvancedSection = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
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
    const fieldAssertion = state.assertion?.fieldAssertion;
    const parameters = state.parameters?.datasetFieldParameters;
    const isFieldValuesAssertion = fieldAssertion?.type === FieldAssertionType.FieldValues;
    const isFieldMetricAssertion = fieldAssertion?.type === FieldAssertionType.FieldMetric;
    const { isTestAssertionModalVisible, handleTestAssertionSubmit, hideTestAssertionModal } = useTestAssertionModal();

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
                {isFieldValuesAssertion && fieldAssertion?.fieldValuesAssertion?.field?.path && (
                    <>
                        <FieldValuesParameterBuilder value={state} onChange={updateState} />
                        <FieldNullCheckBuilder value={state} onChange={updateState} />
                    </>
                )}
                {isFieldMetricAssertion && fieldAssertion?.fieldMetricAssertion?.field?.path && (
                    <FieldMetricBuilder value={state} onChange={updateState} />
                )}
                <FieldRowCheckBuilder value={state} onChange={updateState} />
                <Section>
                    <Collapse>
                        <Collapse.Panel key="Advanced" header="Advanced">
                            <AdvancedSection>
                                {isFieldMetricAssertion && (
                                    <FieldMetricSourceBuilder value={state} onChange={updateState} />
                                )}
                                <FieldFilterBuilder
                                    value={fieldAssertion?.filter as DatasetFilter}
                                    onChange={updateFilter}
                                    sourceType={parameters?.sourceType as DatasetFieldAssertionSourceType}
                                />
                                {isFieldValuesAssertion && (
                                    <FieldErrorThresholdBuilder value={state} onChange={updateState} />
                                )}
                            </AdvancedSection>
                        </Collapse.Panel>
                    </Collapse>
                </Section>
            </div>
            <Controls>
                <Button onClick={prev}>Back</Button>
                <ControlsGroup>
                    <Button onClick={handleTestAssertionSubmit}>Try it out</Button>
                    <Button type="primary" onClick={() => goTo(AssertionBuilderStep.CONFIGURE_ACTIONS)}>
                        Next
                    </Button>
                </ControlsGroup>
            </Controls>
            <TestAssertionModal
                visible={isTestAssertionModalVisible}
                handleClose={hideTestAssertionModal}
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
