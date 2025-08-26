import { Form, Input } from 'antd';
import React, { useEffect } from 'react';
import styled from 'styled-components';

import { onValueChange } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/field/inputs/utils';
import { getFieldAssertionTypeKey } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/field/utils';
import { AssertionMonitorBuilderState } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';

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
