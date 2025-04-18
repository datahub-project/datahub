import { Icon } from '@src/alchemy-components';
import { Form, FormInstance } from 'antd';
import { Tooltip } from '@components';
import React from 'react';
import { RequiredFieldContainer, StyledCheckbox, StyledLabel } from '../styledComponents';

interface Props {
    form: FormInstance;
    isRequired: boolean;
    setIsRequired: React.Dispatch<React.SetStateAction<boolean>>;
}

const RequiredField = ({ form, isRequired, setIsRequired }: Props) => {
    return (
        <Form form={form}>
            <RequiredFieldContainer>
                <Form.Item name="required" style={{ marginBottom: 0 }}>
                    <StyledCheckbox
                        checked={isRequired}
                        onChange={(e) => {
                            setIsRequired(e.target.checked);
                            form.setFieldValue('required', e.target.checked);
                        }}
                    />
                </Form.Item>
                <StyledLabel>Requires response</StyledLabel>
                <Tooltip title="An answer must be provided for this question" showArrow={false}>
                    <Icon icon="Info" color="violet" size="lg" />
                </Tooltip>
            </RequiredFieldContainer>
        </Form>
    );
};

export default RequiredField;
