import { Collapse } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { EvaluationScheduleBuilder } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/common/EvaluationScheduleBuilder';
import { DatasetFreshnessFilterBuilder } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/freshness/DatasetFreshnessFilterBuilder';
import { DatasetFreshnessScheduleBuilder } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/freshness/DatasetFreshnessScheduleBuilder';
import { DatasetFreshnessSourceBuilder } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/freshness/DatasetFreshnessSourceBuilder';
import { AssertionMonitorBuilderState } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/types';

import {
    AssertionEvaluationParametersType,
    AssertionType,
    CronSchedule,
    DatasetFilter,
    DatasetFreshnessAssertionParameters,
    FreshnessAssertionSchedule,
    FreshnessAssertionScheduleType,
} from '@types';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 20px;
`;

type Props = {
    state: AssertionMonitorBuilderState;
    updateState: (state: AssertionMonitorBuilderState) => void;
    disabled?: boolean;
};

/**
 * Step for defining the Dataset Freshness assertion
 */
export const DatasetFreshnessAssertionBuilder = ({ state, updateState, disabled }: Props) => {
    const freshnessAssertion = state.assertion?.freshnessAssertion;
    const freshnessFilter = freshnessAssertion?.filter;
    const freshnessSchedule = freshnessAssertion?.schedule;
    const freshnessScheduleType = freshnessSchedule?.type;
    const datasetFreshnessParameters = state.parameters?.datasetFreshnessParameters;

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
        <div>
            <DatasetFreshnessScheduleBuilder
                value={freshnessSchedule as FreshnessAssertionSchedule}
                onChange={updateFreshnessSchedule}
                disabled={disabled}
            />

            <EvaluationScheduleBuilder
                headerLabel={
                    state.assertion?.freshnessAssertion?.schedule?.type === FreshnessAssertionScheduleType.FixedInterval
                        ? `As of...`
                        : `Schedule checks at...`
                }
                value={state.schedule}
                onChange={updateAssertionSchedule}
                assertionType={AssertionType.Freshness}
                disabled={disabled}
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
                            disabled={disabled}
                        />
                        <DatasetFreshnessFilterBuilder
                            value={freshnessFilter as DatasetFilter}
                            onChange={updateAssertionSqlFilter}
                            sourceType={datasetFreshnessParameters?.sourceType}
                            disabled={disabled}
                        />
                    </Collapse.Panel>
                </Collapse>
            </Section>
        </div>
    );
};
