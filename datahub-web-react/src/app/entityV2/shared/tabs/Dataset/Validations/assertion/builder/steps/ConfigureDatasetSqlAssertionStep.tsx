import React from 'react';
import styled from 'styled-components';
import { Button } from 'antd';
import { AssertionBuilderStep, StepProps } from '../types';
import { EvaluationScheduleBuilder } from './freshness/EvaluationScheduleBuilder';
import { AssertionType, CreateSqlAssertionInput, CronSchedule } from '../../../../../../../../../types.generated';
import { SqlQueryBuilder } from './sql/SqlQueryBuilder';
import { DescriptionBuilder } from './sql/DescriptionBuilder';
import { SqlEvaluationBuilder } from './sql/SqlEvaluationBuilder';
import { TestAssertionModal } from './preview/TestAssertionModal';
import { builderStateToCreateSqlAssertionVariables } from '../utils';
import { useTestAssertionModal } from './utils';

const Step = styled.div`
    height: 100%;
    display: flex;
    flex-direction: column;
    justify-content: space-between;
`;

const Form = styled.div``;

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
 * Step for defining the Dataset SQL assertion
 */
export const ConfigureDatasetSqlAssertionStep = ({ state, updateState, goTo, prev }: StepProps) => {
    const sqlAssertion = state.assertion?.sqlAssertion;
    const { isTestAssertionModalVisible, handleTestAssertionSubmit, hideTestAssertionModal } = useTestAssertionModal();

    const updateAssertionSchedule = (schedule: CronSchedule) => {
        updateState({
            ...state,
            schedule,
        });
    };

    const updateAssertionStatement = (statement: string) => {
        updateState({
            ...state,
            assertion: {
                ...state.assertion,
                sqlAssertion: {
                    ...state.assertion?.sqlAssertion,
                    statement,
                },
            },
        });
    };

    const updateAssertionDescription = (description: string) => {
        updateState({
            ...state,
            assertion: {
                ...state.assertion,
                description,
            },
        });
    };

    return (
        <Step>
            <Form>
                <EvaluationScheduleBuilder
                    value={state.schedule as CronSchedule}
                    onChange={updateAssertionSchedule}
                    assertionType={AssertionType.Sql}
                    showAdvanced={false}
                />
                <SqlQueryBuilder value={sqlAssertion?.statement} onChange={updateAssertionStatement} />
                <SqlEvaluationBuilder value={state} onChange={updateState} />
                <DescriptionBuilder value={state.assertion?.description} onChange={updateAssertionDescription} />
            </Form>
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
                    type: AssertionType.Sql,
                    connectionUrn: state.platformUrn as string,
                    sqlTestInput: builderStateToCreateSqlAssertionVariables(state).input as CreateSqlAssertionInput,
                }}
            />
        </Step>
    );
};
