import { useReactiveVar } from '@apollo/client';
import { Modal, toast } from '@components';
import React, { useCallback, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Redirect } from 'react-router';

import analytics, { EventType } from '@app/analytics';
import { isLoggedInVar } from '@app/auth/checkAuthStatus';
import ResetCredentialsForm from '@app/auth/resetCredentialsV2/ResetCredentialsForm';
import ModalHeader from '@app/auth/shared/ModalHeader';
import { confirmPassword, password, required } from '@app/auth/shared/shared.utils';
import { ResetCredentialsFormValues } from '@app/auth/shared/types';
import { useAuthForm } from '@app/auth/shared/useAuthForm';
import { useLoadingToast } from '@app/auth/shared/useLoadingToast';
import useGetResetTokenFromUrlParams from '@app/auth/useGetResetTokenFromUrlParams';
import { useAppConfig } from '@app/useAppConfig';
import { PageRoutes } from '@conf/Global';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

export default function ResetCredentialsModal() {
    const { t } = useTranslation('auth');
    const isLoggedIn = useReactiveVar(isLoggedInVar);
    const resetToken = useGetResetTokenFromUrlParams();

    const [loading, setLoading] = useState(false);

    const { refreshContext } = useAppConfig();

    const handleResetCredentials = useCallback(
        (values: ResetCredentialsFormValues) => {
            setLoading(true);
            const requestOptions = {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    email: values.email,
                    password: values.password,
                    resetToken,
                }),
            };
            fetch(resolveRuntimePath('/resetNativeUserCredentials'), requestOptions)
                .then(async (response) => {
                    if (!response.ok) {
                        const data = await response.json();
                        const error = (data && data.message) || response.status;
                        return Promise.reject(error);
                    }
                    isLoggedInVar(true);
                    refreshContext();
                    analytics.event({ type: EventType.ResetCredentialsEvent });
                    return Promise.resolve();
                })
                .catch((_) => {
                    toast.error(t('reset.failed'));
                })
                .finally(() => setLoading(false));
        },
        [refreshContext, resetToken, t],
    );

    const form = useAuthForm<ResetCredentialsFormValues>(
        { email: '', password: '', confirmPassword: '' },
        {
            email: required(t('emailRequired')),
            password: password({ required: t('passwordRequired'), tooShort: t('passwordHint') }),
            confirmPassword: confirmPassword({
                required: t('confirmPasswordRequired'),
                mismatch: t('passwordsDoNotMatch'),
            }),
        },
        handleResetCredentials,
    );

    useLoadingToast(loading, t('reset.loading'));

    if (isLoggedIn && !loading) {
        return <Redirect to={`${PageRoutes.ROOT}`} />;
    }

    return (
        <Modal
            title={<ModalHeader />}
            buttons={[
                {
                    text: t('reset.submitButton'),
                    onClick: form.submit,
                    disabled: form.isSubmitDisabled,
                    buttonDataTestId: 'reset-password',
                },
            ]}
            onCancel={() => {}}
            mask={false}
            closable={false}
            width="533px"
        >
            <ResetCredentialsForm form={form} />
        </Modal>
    );
}
