import React, { useCallback, useState } from 'react';
import * as QueryString from 'query-string';
import { Input, Button, Form, message, Image } from 'antd';
import { UserOutlined, LockOutlined } from '@ant-design/icons';
import { useReactiveVar } from '@apollo/client';
import styled, { useTheme } from 'styled-components';
import { Redirect, useLocation } from 'react-router';
import styles from './login.module.css';
import { Message } from '../shared/Message';
import { isLoggedInVar } from './checkAuthStatus';
import analytics, { EventType } from '../analytics';
import { useAppConfig } from '../useAppConfig';

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

export type LogInProps = Record<string, never>;

export const LogIn: React.VFC<LogInProps> = () => {
    const isLoggedIn = useReactiveVar(isLoggedInVar);
    const location = useLocation();

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
                .catch((error) => {
                    message.error(`Failed to log in! ${error}`);
                })
                .finally(() => setLoading(false));
        },
        [refreshContext],
    );

    if (isLoggedIn) {
        const params = QueryString.parse(location.search);
        const maybeRedirectUri = params.redirect_uri;
        return <Redirect to={(maybeRedirectUri && decodeURIComponent(maybeRedirectUri as string)) || '/'} />;
    }

    return (
        <div className={styles.login_page}>
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
                </div>
            </div>
        </div>
    );
};
