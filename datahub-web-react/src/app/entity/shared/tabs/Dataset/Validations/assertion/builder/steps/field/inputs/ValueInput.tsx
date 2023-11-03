import React, { useEffect } from 'react';
import { Form, Input } from 'antd';
import styled from 'styled-components';
import { AssertionMonitorBuilderState } from '../../../types';
import { onValueChange } from './utils';
import { getFieldAssertionTypeKey } from '../utils';

const StyledFormItem = styled(Form.Item)`
    width: 200px;
    margin: 0;
`;

type Props = {
    value: AssertionMonitorBuilderState;
    onChange: (newState: AssertionMonitorBuilderState) => void;
    inputType?: string;
    disabled?: boolean;
};

export const ValueInput = ({ value, onChange, inputType, disabled }: Props) => {
    const form = Form.useFormInstance();
    const fieldAssertionType = value.assertion?.fieldAssertion?.type;
    const fieldAssertionKey = getFieldAssertionTypeKey(fieldAssertionType);
    const fieldValue = value.assertion?.fieldAssertion?.[fieldAssertionKey]?.parameters?.value?.value;

    useEffect(() => {
        form.setFieldValue('fieldValue', fieldValue);
    }, [form, fieldValue]);

    return (
        <StyledFormItem name="fieldValue" rules={[{ required: true, message: 'Required' }]}>
            <Input
                type={inputType}
                placeholder="Value"
                onChange={(e) => onValueChange(e.target.value, value, onChange)}
                disabled={disabled}
            />
        </StyledFormItem>
    );
};
