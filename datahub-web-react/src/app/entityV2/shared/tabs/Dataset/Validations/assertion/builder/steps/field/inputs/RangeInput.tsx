import React, { useEffect } from 'react';
import { Form, InputNumber } from 'antd';
import styled from 'styled-components';
import { AssertionMonitorBuilderState } from '../../../types';
import { onRangeValueChange } from './utils';
import { getFieldAssertionTypeKey } from '../utils';

const StyledFormItem = styled(Form.Item)`
    width: 100px;
    margin: 0;
`;

const StyledNumberInput = styled(InputNumber)`
    width: 100%;
`;

type Props = {
    value: AssertionMonitorBuilderState;
    onChange: (newState: AssertionMonitorBuilderState) => void;
    disabled?: boolean;
};

export const RangeInput = ({ value, onChange, disabled }: Props) => {
    const form = Form.useFormInstance();
    const fieldAssertionType = value.assertion?.fieldAssertion?.type;
    const fieldAssertionKey = getFieldAssertionTypeKey(fieldAssertionType);
    const fieldMinValue = value.assertion?.fieldAssertion?.[fieldAssertionKey]?.parameters?.minValue?.value;
    const fieldMaxValue = value.assertion?.fieldAssertion?.[fieldAssertionKey]?.parameters?.maxValue?.value;

    useEffect(() => {
        form.setFieldValue('fieldMinValue', fieldMinValue ? parseFloat(fieldMinValue) : undefined);
    }, [form, fieldMinValue]);

    useEffect(() => {
        form.setFieldValue('fieldMaxValue', fieldMaxValue ? parseFloat(fieldMaxValue) : undefined);
    }, [form, fieldMaxValue]);

    return (
        <>
            <StyledFormItem
                name="fieldMinValue"
                rules={[
                    { required: true, message: 'Required' },
                    ({ getFieldValue }) => ({
                        validator(_, fieldValue) {
                            if (fieldValue >= getFieldValue('fieldMaxValue')) {
                                return Promise.reject(new Error('Must be less than maximum'));
                            }
                            return Promise.resolve();
                        },
                    }),
                ]}
            >
                <StyledNumberInput
                    placeholder="Minimum"
                    onChange={(newValue) => onRangeValueChange('minValue', newValue as number, value, onChange)}
                    disabled={disabled}
                />
            </StyledFormItem>
            <StyledFormItem
                name="fieldMaxValue"
                rules={[
                    { required: true, message: 'Required' },
                    ({ getFieldValue }) => ({
                        validator(_, fieldValue) {
                            if (fieldValue <= getFieldValue('fieldMinValue')) {
                                return Promise.reject(new Error('Must be greater than minimum'));
                            }
                            return Promise.resolve();
                        },
                    }),
                ]}
            >
                <StyledNumberInput
                    placeholder="Maximum"
                    onChange={(newValue) => onRangeValueChange('maxValue', newValue as number, value, onChange)}
                    disabled={disabled}
                />
            </StyledFormItem>
        </>
    );
};
