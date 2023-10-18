import React, { useEffect } from 'react';
import { Form, Input } from 'antd';
import styled from 'styled-components';
import { AssertionMonitorBuilderState } from '../../../types';
import { onValueChange } from './utils';

const StyledFormItem = styled(Form.Item)`
    width: 200px;
    margin: 0;
`;

type Props = {
    value: AssertionMonitorBuilderState;
    onChange: (newState: AssertionMonitorBuilderState) => void;
    inputType?: string;
};

export const ValueInput = ({ value, onChange, inputType }: Props) => {
    const form = Form.useFormInstance();
    const fieldValue = value.assertion?.fieldAssertion?.fieldValuesAssertion?.parameters?.value?.value;

    useEffect(() => {
        form.setFieldValue('fieldValue', fieldValue);
    }, [form, fieldValue]);

    return (
        <StyledFormItem name="fieldValue" rules={[{ required: true, message: 'Required' }]}>
            <Input
                type={inputType}
                placeholder="Value"
                onChange={(e) => onValueChange(e.target.value, value, onChange)}
            />
        </StyledFormItem>
    );
};
