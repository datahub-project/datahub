import React from 'react';
import { Collapse, Form } from 'antd';
import styled from 'styled-components';
import {
    Assertion,
    AssertionType,
    DatasetFieldAssertionSourceType,
    DatasetFilter,
    FieldAssertionType,
    Monitor,
} from '../../../../../../../../../types.generated';
import { EvaluationScheduleBuilder } from '../steps/freshness/EvaluationScheduleBuilder';
import { FieldTypeBuilder } from '../steps/field/FieldTypeBuilder';
import { FieldColumnBuilder } from '../steps/field/FieldColumnBuilder';
import { FieldValuesParameterBuilder } from '../steps/field/FieldValuesParameterBuilder';
import { FieldMetricBuilder } from '../steps/field/FieldMetricBuilder';
import { FieldNullCheckBuilder } from '../steps/field/FieldNullCheckBuilder';
import { FieldRowCheckBuilder } from '../steps/field/FieldRowCheckBuilder';
import { FieldMetricSourceBuilder } from '../steps/field/FieldMetricSourceBuilder';
import { FieldFilterBuilder } from '../steps/field/FieldFilterBuilder';
import { FieldErrorThresholdBuilder } from '../steps/field/FieldErrorThresholdBuilder';
import { fieldAssertionToBuilderState } from '../steps/field/utils';
import { useEntityData } from '../../../../../../EntityContext';

const Section = styled.div`
    padding-bottom: 20px;
`;

const AdvancedSection = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

type Props = {
    assertion: Assertion;
};

/**
 * This component is used to view Field assertion details in read-only mode.
 */
export const FieldAssertionDetails = ({ assertion }: Props) => {
    const { urn, entityData } = useEntityData();
    const fieldAssertion = assertion.info?.fieldAssertion;
    const isFieldValuesAssertion = fieldAssertion?.type === FieldAssertionType.FieldValues;
    const isFieldMetricAssertion = fieldAssertion?.type === FieldAssertionType.FieldMetric;
    const monitor = (assertion as any)?.monitor?.relationships?.[0]?.entity as Monitor;
    const schedule = monitor?.info?.assertionMonitor?.assertions?.[0]?.schedule;
    const parameters = monitor?.info?.assertionMonitor?.assertions?.[0]?.parameters;
    const datasetFieldParameters = parameters?.datasetFieldParameters;
    const platformUrn = entityData?.platform?.urn as string;
    const state = fieldAssertionToBuilderState(assertion, monitor, urn, platformUrn);

    return (
        <Form
            initialValues={{
                changedRowsColumn: datasetFieldParameters?.changedRowsField?.path,
            }}
        >
            <EvaluationScheduleBuilder
                value={schedule}
                assertionType={AssertionType.Field}
                onChange={() => {}}
                showAdvanced={false}
                disabled
            />
            <FieldTypeBuilder value={state} onChange={() => {}} disabled />
            <FieldColumnBuilder value={state} onChange={() => {}} disabled />
            {isFieldValuesAssertion && fieldAssertion?.fieldValuesAssertion?.field?.path && (
                <>
                    <FieldValuesParameterBuilder value={state} onChange={() => {}} disabled />
                    <FieldNullCheckBuilder value={state} onChange={() => {}} disabled />
                </>
            )}
            {isFieldMetricAssertion && fieldAssertion?.fieldMetricAssertion?.field?.path && (
                <FieldMetricBuilder value={state} onChange={() => {}} disabled />
            )}
            <FieldRowCheckBuilder value={state} onChange={() => {}} disabled />
            <Section>
                <Collapse>
                    <Collapse.Panel key="Advanced" header="Advanced">
                        <AdvancedSection>
                            {isFieldMetricAssertion && (
                                <FieldMetricSourceBuilder value={state} onChange={() => {}} disabled />
                            )}
                            <FieldFilterBuilder
                                value={fieldAssertion?.filter as DatasetFilter}
                                onChange={() => {}}
                                sourceType={datasetFieldParameters?.sourceType as DatasetFieldAssertionSourceType}
                                disabled
                            />
                            {isFieldValuesAssertion && (
                                <FieldErrorThresholdBuilder value={state} onChange={() => {}} disabled />
                            )}
                        </AdvancedSection>
                    </Collapse.Panel>
                </Collapse>
            </Section>
        </Form>
    );
};
