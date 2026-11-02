import { useReactiveVar } from '@apollo/client';
import { Modal, toast } from '@components';
import * as QueryString from 'query-string';
import React, { useCallback, useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router';
import { useLocation } from 'react-router-dom';

import analytics, { EventType } from '@app/analytics';
import { isLoggedInVar } from '@app/auth/checkAuthStatus';
import ModalHeader from '@app/auth/shared/ModalHeader';
import { confirmPassword, password, required } from '@app/auth/shared/shared.utils';
import { SignupFormValues } from '@app/auth/shared/types';
import { useAuthForm } from '@app/auth/shared/useAuthForm';
import SignupForm from '@app/auth/signupV2/SignupForm';
import useGetInviteTokenFromUrlParams from '@app/auth/useGetInviteTokenFromUrlParams';
import { useAppConfig } from '@app/useAppConfig';
import { PageRoutes } from '@conf/Global';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

import { useAcceptRoleMutation } from '@graphql/mutations.generated';

export default function SignUpModal() {
    const history = useHistory();
    const location = useLocation();

    const { t } = useTranslation('auth');

    const [loading, setLoading] = useState(false);
    const { refreshContext } = useAppConfig();

    const isLoggedIn = useReactiveVar(isLoggedInVar);
    const inviteToken = useGetInviteTokenFromUrlParams();

    useEffect(() => {
        const params = QueryString.parse(location.search, { decode: true });
        if (params.redirect_on_sso) {
            fetch(resolveRuntimePath('/sso'), {
                method: 'HEAD',
                redirect: 'manual',
            })
                .then((response) => {
                    if (response.type === 'opaqueredirect' || response.status === 302) {
                        window.location.href = resolveRuntimePath('/sso');
                    }
                })
                .catch(() => {
                    // SSO not configured or error - stay on signup
                });
        }
    }, [location.search]);

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
                    toast.success(t('signup.acceptedInvite'), { duration: 2 });
                }
            })
            .catch((e) => {
                toast.destroy();
                toast.error(t('signup.acceptInviteFailed', { error: e.message || '' }), { duration: 3 });
            });
    };

    useEffect(() => {
        if (isLoggedIn && !loading) {
            acceptRole();
            history.push(PageRoutes.ROOT);
        }
    });

    const handleSignUp = useCallback(
        (values: SignupFormValues) => {
            setLoading(true);
            const requestOptions = {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    fullName: values.fullName,
                    email: values.email,
                    password: values.password,
                    inviteToken,
                }),
            };
            fetch(resolveRuntimePath('/signUp'), requestOptions)
                .then(async (response) => {
                    if (!response.ok) {
                        const data = await response.json();
                        const error = (data && data.message) || response.status;
                        return Promise.reject(error);
                    }
                    isLoggedInVar(true);
                    refreshContext();
                    analytics.event({ type: EventType.SignUpEvent });
                    return Promise.resolve();
                })
                .catch((_) => {
                    toast.error(t('signup.loginFailed'));
                })
                .finally(() => setLoading(false));
        },
        [refreshContext, inviteToken, t],
    );

    const form = useAuthForm<SignupFormValues>(
        { email: '', fullName: '', password: '', confirmPassword: '' },
        {
            email: required(t('emailRequired')),
            fullName: required(t('fullNameRequired')),
            password: password({ required: t('passwordRequired'), tooShort: t('passwordHint') }),
            confirmPassword: confirmPassword({
                required: t('confirmPasswordRequired'),
                mismatch: t('passwordsDoNotMatch'),
            }),
        },
        handleSignUp,
    );

    return (
        <Modal
            title={<ModalHeader subHeading={t('signup.subHeading')} />}
            buttons={[
                {
                    text: t('signup.submitButton'),
                    onClick: form.submit,
                    disabled: form.isSubmitDisabled,
                    buttonDataTestId: 'sign-up',
                },
            ]}
            onCancel={() => {}}
            mask={false}
            closable={false}
            width="533px"
        >
            <SignupForm form={form} />
        </Modal>
    );
}
