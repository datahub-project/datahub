import { Form, Select, Typography } from 'antd';
import React, { useEffect } from 'react';
import styled from 'styled-components';

import { RangeInput } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/field/inputs/RangeInput';
import { SetInput } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/field/inputs/SetInput';
import { ValueInput } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/field/inputs/ValueInput';
import {
    getFieldMetricOperatorOptions,
    getSelectedFieldMetricOperatorOption,
} from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/field/utils';
import { AssertionMonitorBuilderState } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/types';

import { AssertionStdOperator } from '@types';

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

type Props = {
    value: AssertionMonitorBuilderState;
    onChange: (newState: AssertionMonitorBuilderState) => void;
    disabled?: boolean;
};

export const FieldMetricParameterBuilder = ({ value, onChange, disabled }: Props) => {
    const form = Form.useFormInstance();
    const operator = value.assertion?.fieldAssertion?.fieldMetricAssertion?.operator;
    const options = getFieldMetricOperatorOptions();
    const selectedOption = getSelectedFieldMetricOperatorOption(operator);

    const renderInput = () => {
        if (!operator || selectedOption?.hideParameters || !selectedOption?.parameters) return null;

        if ([AssertionStdOperator.In, AssertionStdOperator.NotIn].includes(operator)) {
            return (
                <SetInput value={value} onChange={onChange} inputType={selectedOption?.inputType} disabled={disabled} />
            );
        }
        if ([AssertionStdOperator.Between].includes(operator)) {
            return <RangeInput value={value} onChange={onChange} disabled={disabled} />;
        }
        return (
            <ValueInput value={value} onChange={onChange} inputType={selectedOption?.inputType} disabled={disabled} />
        );
    };

    const updateOperator = (newOperator: AssertionStdOperator) => {
        const operatorConfig = getSelectedFieldMetricOperatorOption(newOperator);
        onChange({
            ...value,
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
                        disabled={disabled}
                    />
                </StyledFormItem>
                {renderInput()}
            </Row>
        </Section>
    );
};
