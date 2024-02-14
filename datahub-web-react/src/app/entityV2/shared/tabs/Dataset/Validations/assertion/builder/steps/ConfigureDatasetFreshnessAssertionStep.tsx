import React from 'react';
import styled from 'styled-components';
import { Button, Collapse } from 'antd';
import { AssertionBuilderStep, StepProps } from '../types';
import {
    AssertionEvaluationParametersInput,
    AssertionEvaluationParametersType,
    AssertionType,
    CreateFreshnessAssertionInput,
    CronSchedule,
    DatasetFilter,
    DatasetFreshnessAssertionParameters,
    FreshnessAssertionSchedule,
    FreshnessAssertionScheduleType,
} from '../../../../../../../../../types.generated';
import { EvaluationScheduleBuilder } from './freshness/EvaluationScheduleBuilder';
import { DatasetFreshnessSourceBuilder } from './freshness/DatasetFreshnessSourceBuilder';
import { DatasetFreshnessFilterBuilder } from './freshness/DatasetFreshnessFilterBuilder';
import { DatasetFreshnessScheduleBuilder } from './freshness/DatasetFreshnessScheduleBuilder';
import { TestAssertionModal } from './preview/TestAssertionModal';
import { builderStateToCreateFreshnessAssertionVariables } from '../utils';
import { useTestAssertionModal } from './utils';

const Step = styled.div`
    height: 100%;
    display: flex;
    flex-direction: column;
    justify-content: space-between;
`;

const Form = styled.div``;

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
 * Step for defining the Dataset Freshness assertion
 */
export const ConfigureDatasetFreshnessAssertionStep = ({ state, updateState, goTo, prev }: StepProps) => {
    const freshnessAssertion = state.assertion?.freshnessAssertion;
    const freshnessFilter = freshnessAssertion?.filter;
    const freshnessSchedule = freshnessAssertion?.schedule;
    const freshnessScheduleType = freshnessSchedule?.type;
    const datasetFreshnessParameters = state.parameters?.datasetFreshnessParameters;
    const { isTestAssertionModalVisible, handleTestAssertionSubmit, hideTestAssertionModal } = useTestAssertionModal();

    const updateDatasetFreshnessAssertionParameters = (parameters: DatasetFreshnessAssertionParameters) => {
        updateState({
            ...state,
            parameters: {
                type: AssertionEvaluationParametersType.DatasetFreshness,
                datasetFreshnessParameters: {
                    sourceType: parameters.sourceType,
                    auditLog: parameters.auditLog as any,
                    field: parameters.field as any,
                },
            },
        });
    };

    const updateAssertionSqlFilter = (filter?: DatasetFilter) => {
        updateState({
            ...state,
            assertion: {
                ...state.assertion,
                freshnessAssertion: {
                    ...state.assertion?.freshnessAssertion,
                    filter,
                },
            },
        });
    };

    const updateAssertionSchedule = (schedule: CronSchedule) => {
        // when the schedule changes, also update the freshness assertion cron schedule
        updateState({
            ...state,
            schedule,
            assertion: {
                ...state.assertion,
                freshnessAssertion: {
                    ...state.assertion?.freshnessAssertion,
                    schedule: {
                        ...state.assertion?.freshnessAssertion?.schedule,
                        cron: schedule,
                    },
                },
            },
        });
    };

    const updateFreshnessSchedule = (schedule: FreshnessAssertionSchedule) => {
        updateState({
            ...state,
            assertion: {
                ...state.assertion,
                freshnessAssertion: {
                    ...state.assertion?.freshnessAssertion,
                    schedule,
                },
            },
        });
    };

    return (
        <Step>
            <Form>
                <EvaluationScheduleBuilder
                    value={state.schedule as CronSchedule}
                    onChange={updateAssertionSchedule}
                    assertionType={AssertionType.Freshness}
                />
                <DatasetFreshnessScheduleBuilder
                    value={freshnessSchedule as FreshnessAssertionSchedule}
                    onChange={updateFreshnessSchedule}
                />
                <Section>
                    <Collapse>
                        <Collapse.Panel key="Advanced" header="Advanced">
                            <DatasetFreshnessSourceBuilder
                                entityUrn={state.entityUrn as string}
                                platformUrn={state.platformUrn as string}
                                scheduleType={freshnessScheduleType as FreshnessAssertionScheduleType}
                                value={datasetFreshnessParameters as DatasetFreshnessAssertionParameters}
                                onChange={updateDatasetFreshnessAssertionParameters}
                            />
                            <DatasetFreshnessFilterBuilder
                                value={freshnessFilter as DatasetFilter}
                                onChange={updateAssertionSqlFilter}
                                sourceType={datasetFreshnessParameters?.sourceType}
                            />
                        </Collapse.Panel>
                    </Collapse>
                </Section>
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
                    type: AssertionType.Freshness,
                    connectionUrn: state.platformUrn as string,
                    freshnessTestInput: builderStateToCreateFreshnessAssertionVariables(state)
                        .input as CreateFreshnessAssertionInput,
                    parameters: state.parameters as AssertionEvaluationParametersInput,
                }}
            />
        </Step>
    );
};
