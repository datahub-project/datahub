import React, { useEffect } from 'react';
import { Form, Select, Typography } from 'antd';
import styled from 'styled-components';
import useFormInstance from 'antd/lib/form/hooks/useFormInstance';
import { AssertionMonitorBuilderState } from '../../types';
import { getFieldValuesOperatorOptions, getSelectedFieldValuesOperatorOption } from './utils';
import { AssertionStdOperator } from '../../../../../../../../../../types.generated';
import { RangeInput } from './inputs/RangeInput';
import { ValueInput } from './inputs/ValueInput';
import { SetInput } from './inputs/SetInput';

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

export const FieldValuesParameterBuilder = ({ value, onChange, disabled }: Props) => {
    const form = useFormInstance();
    const operator = value.assertion?.fieldAssertion?.fieldValuesAssertion?.operator;
    const fieldType = value.assertion?.fieldAssertion?.fieldValuesAssertion?.field?.type;
    const options = getFieldValuesOperatorOptions(fieldType);
    const selectedOption = getSelectedFieldValuesOperatorOption(fieldType, operator);

    const renderInput = () => {
        if (!operator || selectedOption?.hideParameters || !selectedOption?.parameters) return null;

        if ([AssertionStdOperator.In, AssertionStdOperator.NotIn].includes(operator)) {
            return <SetInput value={value} onChange={onChange} inputType={selectedOption?.inputType} />;
        }
        if ([AssertionStdOperator.Between].includes(operator)) {
            return <RangeInput value={value} onChange={onChange} />;
        }
        return <ValueInput value={value} onChange={onChange} inputType={selectedOption?.inputType} />;
    };

    const updateOperator = (newOperator: AssertionStdOperator) => {
        const operatorConfig = getSelectedFieldValuesOperatorOption(fieldType, newOperator);
        onChange({
            ...value,
            assertion: {
                ...value.assertion,
                fieldAssertion: {
                    ...value.assertion?.fieldAssertion,
                    fieldValuesAssertion: {
                        ...value.assertion?.fieldAssertion?.fieldValuesAssertion,
                        operator: newOperator,
                        parameters: operatorConfig.parameters,
                        transform: operatorConfig.transform,
                        excludeNulls:
                            operatorConfig.excludeNulls ??
                            value.assertion?.fieldAssertion?.fieldValuesAssertion?.excludeNulls,
                    },
                },
            },
        });
    };

    useEffect(() => {
        form.setFieldValue('fieldValuesOperator', operator);
    }, [form, operator]);

    return (
        <Section>
            <Typography.Title level={5}>Pass if every value</Typography.Title>
            <Row>
                <StyledFormItem
                    name="fieldValuesOperator"
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
