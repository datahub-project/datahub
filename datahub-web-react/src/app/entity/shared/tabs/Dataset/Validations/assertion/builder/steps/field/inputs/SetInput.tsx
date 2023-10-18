import React, { useEffect } from 'react';
import { Form, Select } from 'antd';
import styled from 'styled-components';
import { AssertionMonitorBuilderState } from '../../../types';
import { onSetValueChange } from './utils';

const StyledFormItem = styled(Form.Item)`
    width: 200px;
    margin: 0;
`;

type Props = {
    value: AssertionMonitorBuilderState;
    onChange: (newState: AssertionMonitorBuilderState) => void;
    inputType?: string;
};

export const SetInput = ({ value, onChange, inputType }: Props) => {
    const form = Form.useFormInstance();
    const fieldValue = value.assertion?.fieldAssertion?.fieldValuesAssertion?.parameters?.value?.value;

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
            />
        </StyledFormItem>
    );
};
