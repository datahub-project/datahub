import { useReactiveVar } from '@apollo/client';
import { Modal, toast } from '@components';
import * as QueryString from 'query-string';
import React, { useCallback, useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Redirect, useLocation } from 'react-router';

import analytics, { EventType } from '@app/analytics';
import { isLoggedInVar } from '@app/auth/checkAuthStatus';
import LoginForm from '@app/auth/loginV2/LoginForm';
import ModalHeader from '@app/auth/shared/ModalHeader';
import { LoginFormValues } from '@app/auth/shared/types';
import { useAuthForm } from '@app/auth/shared/useAuthForm';
import { useLoadingToast } from '@app/auth/shared/useLoadingToast';
import { useAppConfig } from '@app/useAppConfig';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

const REDIRECT_ERROR_TOAST_KEY = 'login-redirect-error';

export default function LoginModal() {
    const { t } = useTranslation('auth');
    const isLoggedIn = useReactiveVar(isLoggedInVar);
    const location = useLocation();
    const params = QueryString.parse(location.search, { decode: true });
    const redirectError = Array.isArray(params.error_msg) ? params.error_msg[0] : params.error_msg;

    const { refreshContext } = useAppConfig();

    const [loading, setLoading] = useState(false);

    const handleLogin = useCallback(
        (values: LoginFormValues) => {
            setLoading(true);
            const requestOptions = {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username: values.username, password: values.password }),
            };

            fetch(resolveRuntimePath('/logIn'), requestOptions)
                .then(async (response) => {
                    if (!response.ok) {
                        const data = await response.json();
                        const error = (data && data.message) || response.status;
                        return Promise.reject(error);
                    }
                    isLoggedInVar(true);
                    refreshContext();
                    analytics.event({ type: EventType.LogInEvent });
                    return Promise.resolve();
                })
                .catch((e) => {
                    toast.error(t('login.failed', { error: e }));
                })
                .finally(() => setLoading(false));
        },
        [refreshContext, t],
    );

    const form = useAuthForm<LoginFormValues>(
        { username: '', password: '' },
        {
            username: (value) => (value ? undefined : t('usernameRequired')),
            password: (value) => (value ? undefined : t('passwordRequired')),
        },
        handleLogin,
    );

    useLoadingToast(loading, t('login.loading'));

    useEffect(() => {
        if (!redirectError) {
            return undefined;
        }
        toast.error(redirectError, { key: REDIRECT_ERROR_TOAST_KEY, duration: 0 });
        return () => toast.destroy(REDIRECT_ERROR_TOAST_KEY);
    }, [redirectError]);

    if (isLoggedIn) {
        const maybeRedirectUri = params.redirect_uri;
        // NOTE we do not decode the redirect_uri because it is already decoded by QueryString.parse
        return <Redirect to={maybeRedirectUri || '/'} />;
    }

    const handleSSOLogin = () => {
        window.location.href = resolveRuntimePath('/sso');
    };

    return (
        <Modal
            title={<ModalHeader />}
            buttons={[
                {
                    text: t('login.ssoButton'),
                    onClick: handleSSOLogin,
                    variant: 'text',
                    color: 'gray',
                },
                {
                    text: t('login.submitButton'),
                    onClick: form.submit,
                    disabled: form.isSubmitDisabled,
                    buttonDataTestId: 'sign-in',
                },
            ]}
            onCancel={() => {}}
            mask={false}
            closable={false}
            width="533px"
        >
            <LoginForm form={form} />
        </Modal>
    );
}
