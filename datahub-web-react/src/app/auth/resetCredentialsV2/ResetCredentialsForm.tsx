import { Input } from '@components';
import { Form, FormInstance } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { ResetCredentialsFormValues } from '@app/auth/shared/types';
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
    handleSubmit: (values: ResetCredentialsFormValues) => void;
    onFormChange: () => void;
    isSubmitDisabled: boolean;
}

export default function ResetCredentialsForm({ form, handleSubmit, onFormChange, isSubmitDisabled }: Props) {
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
                    <FieldLabel label={t('emailLabel')} required />
                    <Form.Item rules={[{ required: true, message: t('emailRequired') }]} name="email">
                        <Input placeholder={t('emailPlaceholder')} inputTestId="email" />
                    </Form.Item>
                </ItemContainer>

                <ItemContainer>
                    <FieldLabel label={t('passwordLabel')} required />
                    <Form.Item
                        rules={[
                            { required: true, message: t('passwordRequired') },
                            ({ getFieldValue }) => ({
                                validator() {
                                    if (getFieldValue('password').length < 8) {
                                        return Promise.reject(new Error(t('passwordHint')));
                                    }
                                    return Promise.resolve();
                                },
                            }),
                        ]}
                        name="password"
                    >
                        <Input placeholder="********" type="password" inputTestId="password" />
                    </Form.Item>
                </ItemContainer>
                <ItemContainer>
                    <FieldLabel label={t('confirmPasswordLabel')} required />
                    <Form.Item
                        rules={[
                            { required: true, message: t('confirmPasswordRequired') },
                            ({ getFieldValue }) => ({
                                validator() {
                                    if (getFieldValue('confirmPassword') !== getFieldValue('password')) {
                                        return Promise.reject(new Error(t('passwordsDoNotMatch')));
                                    }
                                    return Promise.resolve();
                                },
                            }),
                        ]}
                        name="confirmPassword"
                    >
                        <Input placeholder="********" type="password" inputTestId="confirmPassword" />
                    </Form.Item>
                </ItemContainer>
            </Form>
        </FormContainer>
    );
}
