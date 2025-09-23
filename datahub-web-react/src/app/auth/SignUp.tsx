import { LockOutlined, UserOutlined } from '@ant-design/icons';
import { useReactiveVar } from '@apollo/client';
import { Button, Form, Image, Input, Select, message } from 'antd';
import React, { useCallback, useEffect, useState } from 'react';
import { useHistory } from 'react-router-dom';
import styled, { useTheme } from 'styled-components/macro';

import analytics, { EventType } from '@app/analytics';
import { isLoggedInVar } from '@app/auth/checkAuthStatus';
import styles from '@app/auth/login.module.css';
import useGetInviteTokenFromUrlParams from '@app/auth/useGetInviteTokenFromUrlParams';
import { Message } from '@app/shared/Message';
import { useAppConfig } from '@app/useAppConfig';
import { PageRoutes } from '@conf/Global';

import { useAcceptRoleMutation } from '@graphql/mutations.generated';

type FormValues = {
    fullName: string;
    email: string;
    password: string;
    confirmPassword: string;
    title: string;
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

const TitleSelector = styled(Select)`
    .ant-select-selector {
        color: white;
        border: 1px solid #555555 !important;
        background-color: transparent !important;
    }
    .ant-select-arrow {
        color: white;
    }
`;

const StyledFormItem = styled(Form.Item)`
    .ant-input-affix-wrapper-status-error:not(.ant-input-affix-wrapper-disabled):not(
            .ant-input-affix-wrapper-borderless
        ).ant-input-affix-wrapper {
        background-color: transparent;
    }
`;

export type SignUpProps = Record<string, never>;

export const SignUp: React.VFC<SignUpProps> = () => {
    const history = useHistory();
    const isLoggedIn = useReactiveVar(isLoggedInVar);
    const inviteToken = useGetInviteTokenFromUrlParams();

    const themeConfig = useTheme();
    const [loading, setLoading] = useState(false);

    const { refreshContext } = useAppConfig();

    const [acceptRoleMutation] = useAcceptRoleMutation();
    const acceptRole = () => {
        acceptRoleMutation({
            variables: {
                input: {
                    inviteToken,
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({
                        content: `Accepted invite!`,
                        duration: 2,
                    });
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({
                    content: `Failed to accept invite: \n ${e.message || ''}`,
                    duration: 3,
                });
            });
    };

    const handleSignUp = useCallback(
        (values: FormValues) => {
            setLoading(true);
            const requestOptions = {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    fullName: values.fullName,
                    email: values.email,
                    password: values.password,
                    title: values.title,
                    inviteToken,
                }),
            };
            fetch('/signUp', requestOptions)
                .then(async (response) => {
                    if (!response.ok) {
                        const data = await response.json();
                        const error = (data && data.message) || response.status;
                        return Promise.reject(error);
                    }
                    isLoggedInVar(true);
                    refreshContext();
                    analytics.event({ type: EventType.SignUpEvent, title: values.title });
                    return Promise.resolve();
                })
                .catch((_) => {
                    message.error(`Failed to log in! An unexpected error occurred.`);
                })
                .finally(() => setLoading(false));
        },
        [refreshContext, inviteToken],
    );

    useEffect(() => {
        if (isLoggedIn && !loading) {
            acceptRole();
            history.push(PageRoutes.ROOT);
        }
    });

    return (
        <div className={styles.login_page}>
            <div className={styles.login_box}>
                <div className={styles.login_logo_box}>
                    <Image wrapperClassName={styles.logo_image} src={themeConfig.assets?.logoUrl} preview={false} />
                </div>
                <div className={styles.login_form_box}>
                    {loading && <Message type="loading" content="Signing up..." />}
                    <Form onFinish={handleSignUp} layout="vertical">
                        <StyledFormItem
                            rules={[{ required: true, message: 'Please fill in your email' }]}
                            name="email"
                            // eslint-disable-next-line jsx-a11y/label-has-associated-control
                            label={<label style={{ color: 'white' }}>Email</label>}
                        >
                            <FormInput prefix={<UserOutlined />} data-testid="email" />
                        </StyledFormItem>
                        <StyledFormItem
                            rules={[{ required: true, message: 'Please fill in your name' }]}
                            name="fullName"
                            // eslint-disable-next-line jsx-a11y/label-has-associated-control
                            label={<label style={{ color: 'white' }}>Full Name</label>}
                        >
                            <FormInput prefix={<UserOutlined />} data-testid="name" />
                        </StyledFormItem>
                        <StyledFormItem
                            rules={[
                                { required: true, message: 'Please fill in your password' },
                                ({ getFieldValue }) => ({
                                    validator() {
                                        if (getFieldValue('password').length < 8) {
                                            return Promise.reject(
                                                new Error('Your password is fewer than 8 characters'),
                                            );
                                        }
                                        return Promise.resolve();
                                    },
                                }),
                            ]}
                            name="password"
                            // eslint-disable-next-line jsx-a11y/label-has-associated-control
                            label={<label style={{ color: 'white' }}>Password</label>}
                        >
                            <FormInput prefix={<LockOutlined />} type="password" data-testid="password" />
                        </StyledFormItem>
                        <StyledFormItem
                            rules={[
                                { required: true, message: 'Please confirm your password' },
                                ({ getFieldValue }) => ({
                                    validator() {
                                        if (getFieldValue('confirmPassword') !== getFieldValue('password')) {
                                            return Promise.reject(new Error('Your passwords do not match'));
                                        }
                                        return Promise.resolve();
                                    },
                                }),
                            ]}
                            name="confirmPassword"
                            // eslint-disable-next-line jsx-a11y/label-has-associated-control
                            label={<label style={{ color: 'white' }}>Confirm Password</label>}
                        >
                            <FormInput prefix={<LockOutlined />} type="password" data-testid="confirmPassword" />
                        </StyledFormItem>
                        <StyledFormItem
                            rules={[{ required: true, message: 'Please fill in your title!' }]}
                            name="title"
                            // eslint-disable-next-line jsx-a11y/label-has-associated-control
                            label={<label style={{ color: 'white' }}>Title</label>}
                        >
                            <TitleSelector placeholder="Title">
                                <Select.Option value="Data Analyst">Data Analyst</Select.Option>
                                <Select.Option value="Data Engineer">Data Engineer</Select.Option>
                                <Select.Option value="Data Scientist">Data Scientist</Select.Option>
                                <Select.Option value="Software Engineer">Software Engineer</Select.Option>
                                <Select.Option value="Manager">Manager</Select.Option>
                                <Select.Option value="Product Manager">Product Manager</Select.Option>
                                <Select.Option value="Other">Other</Select.Option>
                            </TitleSelector>
                        </StyledFormItem>
                        <StyledFormItem style={{ marginBottom: '0px' }} shouldUpdate>
                            {({ getFieldsValue }) => {
                                const { fullName, email, password, confirmPassword, title } = getFieldsValue() as {
                                    fullName: string;
                                    email: string;
                                    password: string;
                                    confirmPassword: string;
                                    title: string;
                                };
                                const fieldsAreNotEmpty =
                                    !!fullName && !!email && !!password && !!confirmPassword && !!title;
                                const passwordsMatch = password === confirmPassword;
                                const formIsComplete = fieldsAreNotEmpty && passwordsMatch;
                                return (
                                    <Button
                                        type="primary"
                                        block
                                        htmlType="submit"
                                        className={styles.login_button}
                                        disabled={!formIsComplete}
                                    >
                                        Sign Up!
                                    </Button>
                                );
                            }}
                        </StyledFormItem>
                    </Form>
                </div>
            </div>
        </div>
    );
};
