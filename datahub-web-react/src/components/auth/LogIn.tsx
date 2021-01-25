import React, { useCallback } from 'react';
import { Input, Button, Form, message, Image } from 'antd';
import { UserOutlined, LockOutlined } from '@ant-design/icons';
import { ApolloError, useReactiveVar } from '@apollo/client';
import Cookies from 'js-cookie';

import { Redirect } from 'react-router';
import styles from './login.module.css';
import { useLoginMutation } from '../../graphql/auth.generated';
import { Message } from '../generic/Message';
import { isLoggedInVar } from './checkAuthStatus';
import { GlobalCfg } from '../../conf';

type FormValues = {
    username: string;
    password: string;
};

export type LogInProps = Record<string, never>;

export const LogIn: React.VFC<LogInProps> = () => {
    const isLoggedIn = useReactiveVar(isLoggedInVar);
    const [loginMutation, { loading }] = useLoginMutation({});

    const handleLogin = useCallback(
        (values: FormValues) => {
            loginMutation({
                variables: {
                    username: values.username,
                    password: values.password,
                },
            })
                .then(() => {
                    Cookies.set('PLAY_SESSION', 'DUMMY_VALUE');
                    Cookies.set('IS_LOGGED_IN', 'true');
                    isLoggedInVar(true);
                })
                .catch((e: ApolloError) => {
                    message.error(e.message);
                });
        },
        [loginMutation],
    );

    if (isLoggedIn) {
        // Redirect to search only for Demo Purposes
        return <Redirect to="/search?type=dataset&query=test" />;
    }

    return (
        <div className={styles.login_page}>
            <div className={styles.login_box}>
                <Image wrapperClassName={styles.logo_image} src={GlobalCfg.LOGO_IMAGE} preview={false} />
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
