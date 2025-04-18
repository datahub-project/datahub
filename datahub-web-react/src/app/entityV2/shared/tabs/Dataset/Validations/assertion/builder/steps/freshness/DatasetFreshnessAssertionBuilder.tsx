import React from 'react';

import { Collapse } from 'antd';
import styled from 'styled-components';

import {
    AssertionMonitorBuilderState,
    FreshnessAssertionBuilderSchedule,
    FreshnessAssertionBuilderFilter,
    FreshnessAssertionScheduleBuilderTypeOptions,
} from '../../types';
import {
    AssertionEvaluationParametersType,
    AssertionType,
    CronSchedule,
} from '../../../../../../../../../../types.generated';
import { EvaluationScheduleBuilder } from '../common/EvaluationScheduleBuilder';
import { DatasetFreshnessScheduleBuilder } from './DatasetFreshnessScheduleBuilder';
import { DatasetFreshnessSourceBuilder } from './DatasetFreshnessSourceBuilder';
import { DatasetFreshnessFilterBuilder } from './DatasetFreshnessFilterBuilder';
import { FreshnessInfrenceAdjuster } from '../inferred/FreshnessInfrenceAdjuster';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 20px;
`;

type Props = {
    state: AssertionMonitorBuilderState;
    updateState: (state: AssertionMonitorBuilderState) => void;
    disabled?: boolean;
    isEditMode?: boolean;
};

/**
 * Step for defining the Dataset Freshness assertion
 */
export const DatasetFreshnessAssertionBuilder = ({ state, updateState, disabled, isEditMode }: Props) => {
    const freshnessAssertion = state.assertion?.freshnessAssertion;
    const freshnessFilter = freshnessAssertion?.filter;
    const freshnessSchedule = freshnessAssertion?.schedule;
    const freshnessScheduleType = freshnessSchedule?.type;
    const datasetFreshnessParameters = state.parameters?.datasetFreshnessParameters;

    const isAiInferenceSelected = freshnessSchedule?.type === FreshnessAssertionScheduleBuilderTypeOptions.AiInferred;

    const updateDatasetFreshnessAssertionParameters = (
        parameters: Required<AssertionMonitorBuilderState>['parameters']['datasetFreshnessParameters'],
    ) => {
        updateState({
            ...state,
            parameters: {
                type: AssertionEvaluationParametersType.DatasetFreshness,
                datasetFreshnessParameters: {
                    sourceType: parameters?.sourceType,
                    auditLog: parameters?.auditLog,
                    field: parameters?.field,
                },
            },
        });
    };

    const updateAssertionSqlFilter = (filter?: FreshnessAssertionBuilderFilter) => {
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

    const updateFreshnessSchedule = (schedule: FreshnessAssertionBuilderSchedule) => {
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
            {/* Cannot change type for AI inferred freshness assertions */}
            {!(isEditMode && isAiInferenceSelected) && (
                <DatasetFreshnessScheduleBuilder
                    value={freshnessSchedule}
                    onChange={updateFreshnessSchedule}
                    disabled={disabled}
                    isEditMode={isEditMode}
                />
            )}

            {/* No need to select a schedule for AI inferred freshness assertions */}
            {!isAiInferenceSelected && (
                <EvaluationScheduleBuilder
                    headerLabel={
                        state.assertion?.freshnessAssertion?.schedule?.type ===
                        FreshnessAssertionScheduleBuilderTypeOptions.FixedInterval
                            ? `As of...`
                            : `Schedule checks at...`
                    }
                    value={state.schedule}
                    onChange={updateAssertionSchedule}
                    assertionType={AssertionType.Freshness}
                    disabled={disabled}
                />
            )}

            <Section>
                <Collapse>
                    <Collapse.Panel key="Advanced" header="Freshness detection mechanism">
                        {state.entityUrn &&
                            state.platformUrn &&
                            freshnessScheduleType &&
                            datasetFreshnessParameters && (
                                <DatasetFreshnessSourceBuilder
                                    entityUrn={state.entityUrn}
                                    platformUrn={state.platformUrn}
                                    scheduleType={freshnessScheduleType}
                                    value={datasetFreshnessParameters}
                                    onChange={updateDatasetFreshnessAssertionParameters}
                                    disabled={disabled}
                                />
                            )}
                        <DatasetFreshnessFilterBuilder
                            value={freshnessFilter}
                            onChange={updateAssertionSqlFilter}
                            sourceType={datasetFreshnessParameters?.sourceType}
                            disabled={disabled}
                        />
                    </Collapse.Panel>
                </Collapse>
            </Section>

            {isAiInferenceSelected && (
                <FreshnessInfrenceAdjuster state={state} updateState={updateState} disabled={disabled} />
            )}
        </div>
    );
};
