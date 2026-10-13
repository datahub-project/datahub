import { Input } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { ResetCredentialsFormValues } from '@app/auth/shared/types';
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
    form: AuthForm<ResetCredentialsFormValues>;
}

export default function ResetCredentialsForm({ form }: Props) {
    const { t } = useTranslation('auth');

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
                    inputTestId="email"
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
