import React, { useCallback, useState } from 'react';
import * as QueryString from 'query-string';
import { Input, Button, Form, message, Image } from 'antd';
import { UserOutlined, LockOutlined } from '@ant-design/icons';
import { useReactiveVar } from '@apollo/client';
import { useTheme } from 'styled-components';
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
                <Image wrapperClassName={styles.logo_image} src={themeConfig.assets?.logoUrl} preview={false} />
                {loading && <Message type="loading" content="Logging in..." />}
                <h3 className={styles.title}>Connecting you to the data that matters</h3>
                <Form onFinish={handleLogin}>
                    <Form.Item name="username" rules={[{ required: true, message: 'Please input your username!' }]}>
                        <Input prefix={<UserOutlined />} placeholder="Username" />
                    </Form.Item>
                    <Form.Item name="password">
                        <Input prefix={<LockOutlined />} type="password" placeholder="Password" />
                    </Form.Item>
                    <Form.Item>
                        <Button type="primary" block htmlType="submit" className={styles.login_button}>
                            Log in
                        </Button>
                    </Form.Item>
                </Form>
            </div>
        </div>
    );
};
