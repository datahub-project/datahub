import { useReactiveVar } from '@apollo/client';
import { Modal } from '@components';
import { Form, message } from 'antd';
import * as QueryString from 'query-string';
import React, { useCallback, useState } from 'react';
import { Redirect, useLocation } from 'react-router';

import analytics, { EventType } from '@app/analytics';
import { isLoggedInVar } from '@app/auth/checkAuthStatus';
import LoginForm from '@app/auth/loginV2/LoginForm';
import ModalHeader from '@app/auth/shared/ModalHeader';
import { LoginFormValues } from '@app/auth/shared/types';
import { Message } from '@app/shared/Message';
import { useAppConfig } from '@app/useAppConfig';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

export default function LoginModal() {
    const isLoggedIn = useReactiveVar(isLoggedInVar);
    const location = useLocation();
    const params = QueryString.parse(location.search, { decode: true });
    const maybeRedirectError = params.error_msg;

    const { refreshContext } = useAppConfig();

    const [form] = Form.useForm();
    const [isSubmitDisabled, setIsSubmitDisabled] = useState(true);

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
                    message.error(`Failed to log in! ${e}`);
                })
                .finally(() => setLoading(false));
        },
        [refreshContext],
    );

    if (isLoggedIn) {
        const maybeRedirectUri = params.redirect_uri;
        // NOTE we do not decode the redirect_uri because it is already decoded by QueryString.parse
        return <Redirect to={maybeRedirectUri || '/'} />;
    }

    const onFormChange = () => {
        const hasErrors = form.getFieldsError().some(({ errors }) => errors.length > 0);

        const isTouched = form.isFieldsTouched(true);

        setIsSubmitDisabled(hasErrors || !isTouched);
    };

    const handleSSOLogin = () => {
        window.location.href = resolveRuntimePath('/sso');
    };

    return (
        <Modal
            title={<ModalHeader />}
            buttons={[
                {
                    text: 'Sign in with SSO',
                    onClick: handleSSOLogin,
                    variant: 'text',
                    color: 'gray',
                },
                {
                    text: 'Login',
                    onClick: () => form.submit(),
                    disabled: isSubmitDisabled,
                },
            ]}
            onCancel={() => {}}
            mask={false}
            closable={false}
            width="533px"
        >
            {maybeRedirectError && maybeRedirectError.length > 0 && (
                <Message type="error" content={maybeRedirectError} />
            )}
            {loading && <Message type="loading" content="Logging in..." />}
            <LoginForm form={form} handleSubmit={handleLogin} onFormChange={onFormChange} />
        </Modal>
    );
}
