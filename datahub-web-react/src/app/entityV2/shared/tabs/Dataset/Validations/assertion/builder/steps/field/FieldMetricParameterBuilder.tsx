import React, { useEffect } from 'react';
import { Form, Select, Typography } from 'antd';
import styled from 'styled-components';
import { useAppConfig } from '@src/app/useAppConfig';
import {
    AssertionMonitorBuilderState,
    FieldMetricAssertionBuilderOperator,
    FieldMetricAssertionBuilderOperatorOptions,
} from '../../types';
import { getFieldMetricOperatorOptions, getSelectedFieldMetricOperatorOption } from './utils';
import { AssertionStdOperator } from '../../../../../../../../../../types.generated';
import { RangeInput } from './inputs/RangeInput';
import { ValueInput } from './inputs/ValueInput';
import { SetInput } from './inputs/SetInput';
import {
    AI_INFERRED_ASSERTION_DEFAULT_SCHEDULE_CRON,
    AI_INFERRED_ASSERTION_DEFAULT_SCHEDULE_TIMEZONE,
} from '../../constants';

const Section = styled.div`
    margin: 16px 0;
`;

const StyledFormItem = styled(Form.Item)`
    width: 240px;
    margin: 0;
`;

const Row = styled.div`
    display: flex;
    gap: 16px;
`;

const IN_OPERATORS: FieldMetricAssertionBuilderOperator[] = [AssertionStdOperator.In, AssertionStdOperator.NotIn];
const BETWEEN_OPERATORS: FieldMetricAssertionBuilderOperator[] = [AssertionStdOperator.Between];

type Props = {
    value: AssertionMonitorBuilderState;
    onChange: (newState: AssertionMonitorBuilderState) => void;
    disabled?: boolean;
    isEditMode?: boolean;
};

export const FieldMetricParameterBuilder = ({ value, onChange, disabled, isEditMode }: Props) => {
    const form = Form.useFormInstance();
    const operator = value.assertion?.fieldAssertion?.fieldMetricAssertion?.operator;

    const isAiInferenceSelected = operator === FieldMetricAssertionBuilderOperatorOptions.AiInferred;
    const isHardDisabled = isEditMode && isAiInferenceSelected;

    const { onlineSmartAssertionsEnabled } = useAppConfig().config.featureFlags;
    const options = getFieldMetricOperatorOptions({ disableAiInferred: isEditMode || !onlineSmartAssertionsEnabled });
    const selectedOption = getSelectedFieldMetricOperatorOption(operator);

    const renderInput = () => {
        if (!operator || selectedOption?.hideParameters || !selectedOption?.parameters || isAiInferenceSelected)
            return null;

        if (IN_OPERATORS.includes(operator)) {
            return (
                <SetInput value={value} onChange={onChange} inputType={selectedOption?.inputType} disabled={disabled} />
            );
        }
        if (BETWEEN_OPERATORS.includes(operator)) {
            return <RangeInput value={value} onChange={onChange} disabled={disabled} />;
        }
        return (
            <ValueInput value={value} onChange={onChange} inputType={selectedOption?.inputType} disabled={disabled} />
        );
    };

    const updateOperator = (newOperator: FieldMetricAssertionBuilderOperator) => {
        const operatorConfig = getSelectedFieldMetricOperatorOption(newOperator);
        const isAiInferred = newOperator === FieldMetricAssertionBuilderOperatorOptions.AiInferred;
        onChange({
            ...value,
            schedule: isAiInferred
                ? {
                      cron: AI_INFERRED_ASSERTION_DEFAULT_SCHEDULE_CRON,
                      timezone: AI_INFERRED_ASSERTION_DEFAULT_SCHEDULE_TIMEZONE,
                  }
                : value.schedule,
            assertion: {
                ...value.assertion,
                fieldAssertion: {
                    ...value.assertion?.fieldAssertion,
                    fieldMetricAssertion: {
                        ...value.assertion?.fieldAssertion?.fieldMetricAssertion,
                        operator: newOperator,
                        parameters: operatorConfig.parameters,
                    },
                },
            },
        });
    };

    useEffect(() => {
        form.setFieldValue('fieldMetricOperator', operator);
    }, [form, operator]);

    return (
        <Section>
            <Typography.Title level={5}>Pass if metric value</Typography.Title>
            <Row>
                <StyledFormItem
                    name="fieldMetricOperator"
                    rules={[
                        {
                            required: true,
                            message: 'Required',
                        },
                    ]}
                >
                    <Select
                        placeholder="Select passing criteria"
                        onChange={(newOperator) => updateOperator(newOperator)}
                        options={options}
                        disabled={disabled || isHardDisabled}
                    />
                </StyledFormItem>
                {renderInput()}
            </Row>
        </Section>
    );
};
