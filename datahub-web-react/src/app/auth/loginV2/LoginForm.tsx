import { Input } from '@components';
import { Form, FormInstance } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
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
    isSubmitDisabled: boolean;
}

export default function LoginForm({ form, handleSubmit, onFormChange, isSubmitDisabled }: Props) {
    const { t } = useTranslation('auth');

    const handleKeyDown = (e: React.KeyboardEvent) => {
        if (e.key === 'Enter' && !isSubmitDisabled) {
            form.submit();
        }
    };

    return (
        <FormContainer>
            <Form form={form} onFinish={handleSubmit} onFieldsChange={onFormChange} onKeyDown={handleKeyDown}>
                <ItemContainer>
                    <FieldLabel label={t('usernameLabel')} required />
                    <Form.Item rules={[{ required: true, message: t('usernameRequired') }]} name="username">
                        <Input placeholder={t('usernamePlaceholder')} inputTestId="username" />
                    </Form.Item>
                </ItemContainer>

                <ItemContainer>
                    <FieldLabel label={t('passwordLabel')} required />
                    <Form.Item rules={[{ required: true, message: t('passwordRequired') }]} name="password">
                        <Input placeholder="********" type="password" inputTestId="password" />
                    </Form.Item>
                </ItemContainer>
            </Form>
        </FormContainer>
    );
}
