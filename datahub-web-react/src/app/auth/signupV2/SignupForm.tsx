import { Input } from '@components';
import React, { useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocation } from 'react-router';
import styled from 'styled-components';

import { SignupFormValues } from '@app/auth/shared/types';
import { AuthForm } from '@app/auth/shared/useAuthForm';
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
    form: AuthForm<SignupFormValues>;
}

export default function SignupForm({ form }: Props) {
    const location = useLocation();
    const { t } = useTranslation('auth');

    const searchParams = new URLSearchParams(location.search);

    const emailFromQuery = searchParams.get('email');
    const firstNameFromQuery = searchParams.get('first_name');
    const lastNameFromQuery = searchParams.get('last_name');

    const isEmailFromQuery = Boolean(emailFromQuery);
    const { setFieldValues } = form;

    useEffect(() => {
        const prefilled: Partial<SignupFormValues> = {};
        if (emailFromQuery) {
            prefilled.email = emailFromQuery;
        }
        if (firstNameFromQuery || lastNameFromQuery) {
            prefilled.fullName = `${firstNameFromQuery ?? ''} ${lastNameFromQuery ?? ''}`.trim();
        }
        if (Object.keys(prefilled).length > 0) {
            setFieldValues(prefilled);
        }
    }, [emailFromQuery, firstNameFromQuery, lastNameFromQuery, setFieldValues]);

    const handleKeyDown = (e: React.KeyboardEvent) => {
        if (e.key === 'Enter') {
            form.submit();
        }
    };

    return (
        <FormContainer onKeyDown={handleKeyDown}>
            <ItemContainer>
                <FieldLabel label={t('emailLabel')} required />
                <Input
                    value={form.values.email}
                    setValue={(value) => form.setFieldValue('email', value)}
                    error={form.errors.email}
                    placeholder={t('emailPlaceholder')}
                    isDisabled={isEmailFromQuery}
                    inputTestId="email"
                />
            </ItemContainer>

            <ItemContainer>
                <FieldLabel label={t('fullNameLabel')} required />
                <Input
                    value={form.values.fullName}
                    setValue={(value) => form.setFieldValue('fullName', value)}
                    error={form.errors.fullName}
                    placeholder={t('fullNamePlaceholder')}
                    inputTestId="name"
                />
            </ItemContainer>

            <ItemContainer>
                <FieldLabel label={t('passwordLabel')} required />
                <Input
                    value={form.values.password}
                    setValue={(value) => form.setFieldValue('password', value)}
                    error={form.errors.password}
                    placeholder="********"
                    type="password"
                    inputTestId="password"
                />
            </ItemContainer>

            <ItemContainer>
                <FieldLabel label={t('confirmPasswordLabel')} required />
                <Input
                    value={form.values.confirmPassword}
                    setValue={(value) => form.setFieldValue('confirmPassword', value)}
                    error={form.errors.confirmPassword}
                    placeholder="********"
                    type="password"
                    inputTestId="confirmPassword"
                />
            </ItemContainer>
        </FormContainer>
    );
}
