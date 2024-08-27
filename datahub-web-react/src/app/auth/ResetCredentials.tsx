import React, { useCallback, useState } from 'react';
import { Input, Button, Form, message, Image } from 'antd';
import { UserOutlined, LockOutlined } from '@ant-design/icons';
import { useReactiveVar } from '@apollo/client';
import styled, { useTheme } from 'styled-components/macro';
import { Redirect } from 'react-router';
import { useTranslation } from 'react-i18next';
import styles from './login.module.css';
import { Message } from '../shared/Message';
import { isLoggedInVar } from './checkAuthStatus';
import analytics, { EventType } from '../analytics';
import { useAppConfig } from '../useAppConfig';
import { PageRoutes } from '../../conf/Global';
import useGetResetTokenFromUrlParams from './useGetResetTokenFromUrlParams';

type FormValues = {
    email: string;
    password: string;
    confirmPassword: string;
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

const StyledFormItem = styled(Form.Item)`
    .ant-input-affix-wrapper-status-error:not(.ant-input-affix-wrapper-disabled):not(
            .ant-input-affix-wrapper-borderless
        ).ant-input-affix-wrapper {
        background-color: transparent;
    }
`;

export type ResetCredentialsProps = Record<string, never>;

export const ResetCredentials: React.VFC<ResetCredentialsProps> = () => {
    const { t } = useTranslation();
    const isLoggedIn = useReactiveVar(isLoggedInVar);
    const resetToken = useGetResetTokenFromUrlParams();

    const themeConfig = useTheme();
    const [loading, setLoading] = useState(false);

    const { refreshContext } = useAppConfig();

    const handleResetCredentials = useCallback(
        (values: FormValues) => {
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
            fetch('/resetNativeUserCredentials', requestOptions)
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
                    message.error(t('authentification.failedToLogIn'));
                })
                .finally(() => setLoading(false));
        },
        [refreshContext, resetToken, t],
    );

    if (isLoggedIn && !loading) {
        return <Redirect to={`${PageRoutes.ROOT}`} />;
    }

    return (
        <div className={styles.login_page}>
            <div className={styles.login_box}>
                <div className={styles.login_logo_box}>
                    <Image wrapperClassName={styles.logo_image} src={themeConfig.assets?.logoUrl} preview={false} />
                </div>
                <div className={styles.login_form_box}>
                    {loading && <Message type="loading" content="Resetting credentials..." />}
                    <Form onFinish={handleResetCredentials} layout="vertical">
                        <StyledFormItem
                            rules={[{ required: true, message: t('form.fillInYourEmail') }]}
                            name="email"
                            // eslint-disable-next-line jsx-a11y/label-has-associated-control
                            label={<label style={{ color: 'white' }}>{t('common.email')}</label>}
                        >
                            <FormInput prefix={<UserOutlined />} data-testid="email" />
                        </StyledFormItem>
                        <StyledFormItem
                            rules={[
                                { required: true, message: t('form.fillInYourPassword') },
                                ({ getFieldValue }) => ({
                                    validator() {
                                        if (getFieldValue('password').length < 8) {
                                            return Promise.reject(new Error(t('form.passwordIsFewerThan8Characters')));
                                        }
                                        return Promise.resolve();
                                    },
                                }),
                            ]}
                            name="password"
                            // eslint-disable-next-line jsx-a11y/label-has-associated-control
                            label={<label style={{ color: 'white' }}>{t('common.password')}</label>}
                        >
                            <FormInput prefix={<LockOutlined />} type="password" data-testid="password" />
                        </StyledFormItem>
                        <StyledFormItem
                            rules={[
                                { required: true, message: t('form.pleaseConfirmYourPassword') },
                                ({ getFieldValue }) => ({
                                    validator() {
                                        if (getFieldValue('confirmPassword') !== getFieldValue('password')) {
                                            return Promise.reject(new Error(t('form.passwordsDoNotMatch')));
                                        }
                                        return Promise.resolve();
                                    },
                                }),
                            ]}
                            name="confirmPassword"
                            // eslint-disable-next-line jsx-a11y/label-has-associated-control
                            label={<label style={{ color: 'white' }}>{t('form.pleaseConfirmYourPassword')}</label>}
                        >
                            <FormInput prefix={<LockOutlined />} type="password" data-testid="confirmPassword" />
                        </StyledFormItem>
                        <StyledFormItem style={{ marginBottom: '0px' }} shouldUpdate>
                            {({ getFieldsValue }) => {
                                const { email, password, confirmPassword } = getFieldsValue() as {
                                    email: string;
                                    password: string;
                                    confirmPassword: string;
                                };
                                const fieldsAreNotEmpty = !!email && !!password && !!confirmPassword;
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
                                        {t('authentification.resetCredentials')}
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
