import { LockOutlined, LoginOutlined, UserOutlined } from '@ant-design/icons';
import { useReactiveVar } from '@apollo/client';
import { Button, Divider, Form, Image, Input, message } from 'antd';
import * as QueryString from 'query-string';
import React, { useCallback, useState } from 'react';
import { Redirect, useLocation } from 'react-router';
import styled, { useTheme } from 'styled-components/macro';

import analytics, { EventType } from '@app/analytics';
import { isLoggedInVar } from '@app/auth/checkAuthStatus';
import styles from '@app/auth/login.module.css';
import { Message } from '@app/shared/Message';
import { useAppConfig } from '@app/useAppConfig';

type FormValues = {
    username: string;
    password: string;
};

const FormInput = styled(Input)`
    &&& {
        height: 32px;
        font-size: 12px;
        border: 1px solid #555555;
        border-radius: 5px;
        background-color: transparent;
        color: white;
        line-height: 1.5715;
    }
    > .ant-input {
        color: white;
        font-size: 14px;
        background-color: transparent;
    }
    > .ant-input:hover {
        color: white;
        font-size: 14px;
        background-color: transparent;
    }
`;

const SsoDivider = styled(Divider)`
    background-color: white;
`;

const SsoButton = styled(Button)`
    &&& {
        align-self: center;
        display: flex;
        justify-content: center;
        align-items: center;
        padding: 5.6px 11px;
        gap: 4px;
    }
`;

const LoginLogo = styled(LoginOutlined)`
    padding-top: 7px;
`;

const SsoTextSpan = styled.span`
    padding-top: 6px;
`;

export type LogInProps = Record<string, never>;

export const LogIn: React.VFC<LogInProps> = () => {
    const isLoggedIn = useReactiveVar(isLoggedInVar);
    const location = useLocation();
    const params = QueryString.parse(location.search, { decode: true });
    const maybeRedirectError = params.error_msg;

    const themeConfig = useTheme();
    const [loading, setLoading] = useState(false);

    const { refreshContext } = useAppConfig();

    const handleLogin = useCallback(
        (values: FormValues) => {
            setLoading(true);
            const requestOptions = {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username: values.username, password: values.password }),
            };
            fetch('/logIn', requestOptions)
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
                .catch((_) => {
                    message.error(`Failed to log in! An unexpected error occurred.`);
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

    return (
        <div className={styles.login_page}>
            {maybeRedirectError && maybeRedirectError.length > 0 && (
                <Message type="error" content={maybeRedirectError} />
            )}
            <div className={styles.login_box}>
                <div className={styles.login_logo_box}>
                    <Image wrapperClassName={styles.logo_image} src={themeConfig.assets?.logoUrl} preview={false} />
                </div>
                <div className={styles.login_form_box}>
                    {loading && <Message type="loading" content="Logging in..." />}
                    <Form onFinish={handleLogin} layout="vertical">
                        <Form.Item
                            name="username"
                            // eslint-disable-next-line jsx-a11y/label-has-associated-control
                            label={<label style={{ color: 'white' }}>Username</label>}
                        >
                            <FormInput prefix={<UserOutlined />} data-testid="username" />
                        </Form.Item>
                        <Form.Item
                            name="password"
                            // eslint-disable-next-line jsx-a11y/label-has-associated-control
                            label={<label style={{ color: 'white' }}>Password</label>}
                        >
                            <FormInput prefix={<LockOutlined />} type="password" data-testid="password" />
                        </Form.Item>
                        <Form.Item style={{ marginBottom: '0px' }} shouldUpdate>
                            {({ getFieldsValue }) => {
                                const { username, password } = getFieldsValue();
                                const formIsComplete = !!username && !!password;
                                return (
                                    <Button
                                        type="primary"
                                        block
                                        htmlType="submit"
                                        className={styles.login_button}
                                        disabled={!formIsComplete}
                                    >
                                        Sign In
                                    </Button>
                                );
                            }}
                        </Form.Item>
                    </Form>
                    <SsoDivider />
                    <SsoButton type="primary" href="/sso" block htmlType="submit" className={styles.sso_button}>
                        <LoginLogo />
                        <SsoTextSpan>Sign in with SSO</SsoTextSpan>
                        <span />
                    </SsoButton>
                </div>
            </div>
        </div>
    );
};
