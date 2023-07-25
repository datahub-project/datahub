import React from 'react';
import styled from 'styled-components';
import { Button, Collapse, Radio, Typography } from 'antd';
import { AssertionBuilderStep, StepProps } from '../types';
import {
    AssertionEvaluationParametersType,
    CronSchedule,
    DatasetFilter,
    DatasetFreshnessAssertionParameters,
    FixedIntervalSchedule,
    FreshnessAssertionScheduleType,
} from '../../../../../../../../../types.generated';
import { FixedIntervalScheduleBuilder } from './freshness/FixedIntervalSchedulerBuilder';
import { CronScheduleBuilder } from './freshness/CronScheduleBuilder';
import { DatasetFreshnessSourceBuilder } from './freshness/DatasetFreshnessSourceBuilder';
import { DatasetFreshnessFilterBuilder } from './freshness/DatasetFreshnessFilterBuilder';

const TypeLabel = styled(Typography.Title)`
    && {
        margin-bottom: 16px;
    }
`;

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

const SourceDescription = styled(Typography.Paragraph)`
    margin-top: 12px;
`;

/**
 * Step for defining the Dataset Freshness assertion
 */
export const ConfigureDatasetFreshnessAssertionStep = ({ state, updateState, goTo, prev }: StepProps) => {
    const freshnessAssertion = state.assertion?.freshnessAssertion;
    const freshnessFilter = freshnessAssertion?.filter;
    const freshnessSchedule = freshnessAssertion?.schedule;
    const freshnessScheduleType = freshnessSchedule?.type;
    const freshnessScheduleCron = freshnessSchedule?.cron;
    const freshnessScheduleFixedInterval = freshnessSchedule?.fixedInterval;
    const datasetFreshnessParameters = state.parameters?.datasetFreshnessParameters;

    const updateScheduleType = (scheduleType: FreshnessAssertionScheduleType) => {
        updateState({
            ...state,
            assertion: {
                ...state.assertion,
                freshnessAssertion: {
                    ...state?.assertion?.freshnessAssertion,
                    schedule: {
                        ...state?.assertion?.freshnessAssertion?.schedule,
                        type: scheduleType,
                    },
                },
            },
        });
    };

    const updateFixedIntervalSchedule = (fixedInterval: FixedIntervalSchedule) => {
        updateState({
            ...state,
            assertion: {
                ...state.assertion,
                freshnessAssertion: {
                    ...state?.assertion?.freshnessAssertion,
                    schedule: {
                        ...state?.assertion?.freshnessAssertion?.schedule,
                        fixedInterval,
                    },
                },
            },
        });
    };

    const updateCronSchedule = (cron: CronSchedule) => {
        updateState({
            ...state,
            assertion: {
                ...state.assertion,
                freshnessAssertion: {
                    ...state.assertion?.freshnessAssertion,
                    schedule: {
                        ...state.assertion?.freshnessAssertion?.schedule,
                        cron,
                    },
                },
            },
        });
    };

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

    return (
        <Step>
            <Form>
                <Section>
                    <TypeLabel level={5}>Type</TypeLabel>
                    <Radio.Group value={freshnessScheduleType} onChange={(e) => updateScheduleType(e.target.value)}>
                        <Radio.Button value={FreshnessAssertionScheduleType.FixedInterval}>Fixed Interval</Radio.Button>
                        <Radio.Button value={FreshnessAssertionScheduleType.Cron}>Schedule</Radio.Button>
                    </Radio.Group>
                    <SourceDescription type="secondary">
                        {freshnessScheduleType === FreshnessAssertionScheduleType.FixedInterval
                            ? 'Define an expected change interval to monitor for this dataset'
                            : 'Define an expected change schedule to monitor for this dataset'}
                    </SourceDescription>
                </Section>
                <Section>
                    {freshnessScheduleType === FreshnessAssertionScheduleType.FixedInterval ? (
                        <FixedIntervalScheduleBuilder
                            value={freshnessScheduleFixedInterval as FixedIntervalSchedule}
                            onChange={updateFixedIntervalSchedule}
                        />
                    ) : (
                        <CronScheduleBuilder
                            title="Expected Change Schedule"
                            value={freshnessScheduleCron as CronSchedule}
                            onChange={updateCronSchedule}
                            actionText="Changes by"
                            descriptionText="Assertion will fail if this dataset has not changed by the schedule timed, or between two consecutive schedule times"
                        />
                    )}
                </Section>
                <Section>
                    <Collapse>
                        <Collapse.Panel key="Advanced" header="Advanced">
                            <DatasetFreshnessSourceBuilder
                                entityUrn={state.entityUrn as string}
                                platformUrn={state.platformUrn as string}
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
                <Button type="primary" onClick={() => goTo(AssertionBuilderStep.CONFIGURE_SCHEDULE)}>
                    Next
                </Button>
            </Controls>
        </Step>
    );
};
