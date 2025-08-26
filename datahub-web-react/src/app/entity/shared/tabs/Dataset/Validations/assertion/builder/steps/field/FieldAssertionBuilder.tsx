import { Collapse } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { EvaluationScheduleBuilder } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/common/EvaluationScheduleBuilder';
import { FieldColumnBuilder } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/field/FieldColumnBuilder';
import { FieldErrorThresholdBuilder } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/field/FieldErrorThresholdBuilder';
import { FieldFilterBuilder } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/field/FieldFilterBuilder';
import { FieldMetricBuilder } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/field/FieldMetricBuilder';
import { FieldMetricSourceBuilder } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/field/FieldMetricSourceBuilder';
import { FieldNullCheckBuilder } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/field/FieldNullCheckBuilder';
import { FieldRowCheckBuilder } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/field/FieldRowCheckBuilder';
import { FieldTypeBuilder } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/field/FieldTypeBuilder';
import { FieldValuesParameterBuilder } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/field/FieldValuesParameterBuilder';
import { AssertionMonitorBuilderState } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/types';

import {
    AssertionType,
    CronSchedule,
    DatasetFieldAssertionSourceType,
    DatasetFilter,
    FieldAssertionType,
} from '@types';

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
    disabled?: boolean;
};

/**
 * Step for defining the Dataset Field assertion
 */
export const FieldAssertionBuilder = ({ state, updateState, disabled }: Props) => {
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
            <FieldTypeBuilder value={state} onChange={updateState} disabled={disabled} />
            <FieldColumnBuilder value={state} onChange={updateState} disabled={disabled} />
            {isFieldValuesAssertion && fieldAssertion?.fieldValuesAssertion?.field?.path && (
                <>
                    <FieldValuesParameterBuilder value={state} onChange={updateState} disabled={disabled} />
                    <FieldNullCheckBuilder value={state} onChange={updateState} disabled={disabled} />
                </>
            )}
            {isFieldMetricAssertion && fieldAssertion?.fieldMetricAssertion?.field?.path && (
                <FieldMetricBuilder value={state} onChange={updateState} disabled={disabled} />
            )}
            <FieldRowCheckBuilder value={state} onChange={updateState} disabled={disabled} />
            <Section>
                <Collapse>
                    <Collapse.Panel key="Advanced" header="Advanced">
                        <AdvancedSection>
                            {isFieldMetricAssertion && (
                                <FieldMetricSourceBuilder value={state} onChange={updateState} disabled={disabled} />
                            )}
                            <FieldFilterBuilder
                                value={fieldAssertion?.filter as DatasetFilter}
                                onChange={updateFilter}
                                sourceType={parameters?.sourceType as DatasetFieldAssertionSourceType}
                                disabled={disabled}
                            />
                            {isFieldValuesAssertion && (
                                <FieldErrorThresholdBuilder value={state} onChange={updateState} disabled={disabled} />
                            )}
                        </AdvancedSection>
                    </Collapse.Panel>
                </Collapse>
            </Section>
            <EvaluationScheduleBuilder
                value={state.schedule}
                onChange={updateAssertionSchedule}
                assertionType={AssertionType.Field}
                showAdvanced={false}
                disabled={disabled}
            />
        </div>
    );
};
