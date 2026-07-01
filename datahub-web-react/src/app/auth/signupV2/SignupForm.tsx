import { Input } from '@components';
import { Form, FormInstance } from 'antd';
import React, { useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocation } from 'react-router';
import styled from 'styled-components';

import { SignupFormValues } from '@app/auth/shared/types';
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
    handleSubmit: (values: SignupFormValues) => void;
    onFormChange: () => void;
    isSubmitDisabled: boolean;
}

export default function SignupForm({ form, handleSubmit, onFormChange, isSubmitDisabled }: Props) {
    const location = useLocation();
    const { t } = useTranslation('auth');

    const searchParams = new URLSearchParams(location.search);

    const emailFromQuery = searchParams.get('email');
    const firstNameFromQuery = searchParams.get('first_name');
    const lastNameFromQuery = searchParams.get('last_name');

    const isEmailFromQuery = Boolean(emailFromQuery);

    useEffect(() => {
        form.setFieldsValue({
            email: emailFromQuery || undefined,
            fullName:
                firstNameFromQuery || lastNameFromQuery
                    ? `${firstNameFromQuery ?? ''} ${lastNameFromQuery ?? ''}`.trim()
                    : undefined,
        });
    }, [emailFromQuery, firstNameFromQuery, lastNameFromQuery, form]);

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
                        <Input placeholder={t('emailPlaceholder')} isDisabled={isEmailFromQuery} inputTestId="email" />
                    </Form.Item>
                </ItemContainer>

                <ItemContainer>
                    <FieldLabel label={t('fullNameLabel')} required />
                    <Form.Item rules={[{ required: true, message: t('fullNameRequired') }]} name="fullName">
                        <Input placeholder={t('fullNamePlaceholder')} inputTestId="name" />
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
