import { Input } from '@components';
import { Form, FormInstance } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { LoginFormValues } from '@app/auth/shared/types';
import { FieldLabel } from '@app/sharedV2/forms/FieldLabel';

const FormContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 32px;
    padding: 0 20px;
`;

const ItemContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

interface Props {
    form: FormInstance;
    handleSubmit: (values: LoginFormValues) => void;
    onFormChange: () => void;
}

export default function LoginForm({ form, handleSubmit, onFormChange }: Props) {
    return (
        <FormContainer>
            <Form form={form} onFinish={handleSubmit} onFieldsChange={onFormChange}>
                <ItemContainer>
                    <FieldLabel label="Username" required />
                    <Form.Item rules={[{ required: true, message: 'Please fill in your username' }]} name="username">
                        <Input placeholder="Enter username" />
                    </Form.Item>
                </ItemContainer>

                <ItemContainer>
                    <FieldLabel label="Password" required />
                    <Form.Item rules={[{ required: true, message: 'Please fill in your password' }]} name="password">
                        <Input placeholder="********" type="password" />
                    </Form.Item>
                </ItemContainer>
            </Form>
        </FormContainer>
    );
}
