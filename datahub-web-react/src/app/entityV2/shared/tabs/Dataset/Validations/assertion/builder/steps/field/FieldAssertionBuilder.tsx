import React from 'react';

import { Collapse } from 'antd';
import styled from 'styled-components';

import { AssertionMonitorBuilderState } from '../../types';
import {
    AssertionType,
    CronSchedule,
    DatasetFieldAssertionSourceType,
    DatasetFilter,
    FieldAssertionType,
} from '../../../../../../../../../../types.generated';
import { EvaluationScheduleBuilder } from '../freshness/EvaluationScheduleBuilder';
import { FieldTypeBuilder } from './FieldTypeBuilder';
import { FieldColumnBuilder } from './FieldColumnBuilder';
import { FieldValuesParameterBuilder } from './FieldValuesParameterBuilder';
import { FieldNullCheckBuilder } from './FieldNullCheckBuilder';
import { FieldMetricBuilder } from './FieldMetricBuilder';
import { FieldRowCheckBuilder } from './FieldRowCheckBuilder';
import { FieldMetricSourceBuilder } from './FieldMetricSourceBuilder';
import { FieldFilterBuilder } from './FieldFilterBuilder';
import { FieldErrorThresholdBuilder } from './FieldErrorThresholdBuilder';
import { AssertionActionsSection } from '../actions/AssertionActionsSection';

const Section = styled.div`
    padding-bottom: 20px;
`;

const AdvancedSection = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

type Props = {
    state: AssertionMonitorBuilderState;
    updateState: (state: AssertionMonitorBuilderState) => void;
    editing?: boolean;
};

/**
 * Step for defining the Dataset Field assertion
 */
export const FieldAssertionBuilder = ({ state, updateState, editing }: Props) => {
    const fieldAssertion = state.assertion?.fieldAssertion;
    const parameters = state.parameters?.datasetFieldParameters;
    const isFieldValuesAssertion = fieldAssertion?.type === FieldAssertionType.FieldValues;
    const isFieldMetricAssertion = fieldAssertion?.type === FieldAssertionType.FieldMetric;

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
        <div>
            <EvaluationScheduleBuilder
                value={state.schedule}
                onChange={updateAssertionSchedule}
                assertionType={AssertionType.Field}
                showAdvanced={false}
                disabled={!editing}
            />
            <FieldTypeBuilder value={state} onChange={updateState} disabled={!editing} />
            <FieldColumnBuilder value={state} onChange={updateState} disabled={!editing} />
            {isFieldValuesAssertion && fieldAssertion?.fieldValuesAssertion?.field?.path && (
                <>
                    <FieldValuesParameterBuilder value={state} onChange={updateState} disabled={!editing} />
                    <FieldNullCheckBuilder value={state} onChange={updateState} disabled={!editing} />
                </>
            )}
            {isFieldMetricAssertion && fieldAssertion?.fieldMetricAssertion?.field?.path && (
                <FieldMetricBuilder value={state} onChange={updateState} disabled={!editing} />
            )}
            <FieldRowCheckBuilder value={state} onChange={updateState} disabled={!editing} />
            <Section>
                <Collapse>
                    <Collapse.Panel key="Advanced" header="Advanced">
                        <AdvancedSection>
                            {isFieldMetricAssertion && (
                                <FieldMetricSourceBuilder value={state} onChange={updateState} disabled={!editing} />
                            )}
                            <FieldFilterBuilder
                                value={fieldAssertion?.filter as DatasetFilter}
                                onChange={updateFilter}
                                sourceType={parameters?.sourceType as DatasetFieldAssertionSourceType}
                                disabled={!editing}
                            />
                            {isFieldValuesAssertion && (
                                <FieldErrorThresholdBuilder value={state} onChange={updateState} disabled={!editing} />
                            )}
                        </AdvancedSection>
                    </Collapse.Panel>
                </Collapse>
            </Section>
            <AssertionActionsSection state={state} updateState={updateState} editing={editing} />
        </div>
    );
};
