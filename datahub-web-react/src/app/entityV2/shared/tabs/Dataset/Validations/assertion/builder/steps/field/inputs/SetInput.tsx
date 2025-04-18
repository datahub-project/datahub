import React, { useEffect } from 'react';
import { Form, Select } from 'antd';
import styled from 'styled-components';
import { AssertionMonitorBuilderState } from '../../../types';
import { onSetValueChange } from './utils';
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

export const SetInput = ({ value, onChange, inputType, disabled }: Props) => {
    const form = Form.useFormInstance();
    const fieldAssertionType = value.assertion?.fieldAssertion?.type;
    const fieldAssertionKey = getFieldAssertionTypeKey(fieldAssertionType);
    const fieldValue = value.assertion?.fieldAssertion?.[fieldAssertionKey]?.parameters?.value?.value;

    useEffect(() => {
        form.setFieldValue('fieldValue', JSON.parse(fieldValue || '[]'));
    }, [form, fieldValue]);

    return (
        <StyledFormItem name="fieldValue" rules={[{ required: true, message: 'Required' }]}>
            <Select
                mode="tags"
                placeholder="Press Enter to add values"
                onChange={(newValues) => onSetValueChange(newValues, value, onChange, inputType)}
                open={false}
                disabled={disabled}
            />
        </StyledFormItem>
    );
};
