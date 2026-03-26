import { useReactiveVar } from '@apollo/client';
import { Modal } from '@components';
import { Form, message } from 'antd';
import React, { useCallback, useEffect, useState } from 'react';
import { useHistory } from 'react-router';

import analytics, { EventType } from '@app/analytics';
import { isLoggedInVar } from '@app/auth/checkAuthStatus';
import ModalHeader from '@app/auth/shared/ModalHeader';
import { SignupFormValues } from '@app/auth/shared/types';
import SignupForm from '@app/auth/signupV2/SignupForm';
import useGetInviteTokenFromUrlParams from '@app/auth/useGetInviteTokenFromUrlParams';
import { useAppConfig } from '@app/useAppConfig';
import { PageRoutes } from '@conf/Global';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

import { useAcceptRoleMutation } from '@graphql/mutations.generated';

export default function SignUpModal() {
    const history = useHistory();

    const [form] = Form.useForm();

    const [loading, setLoading] = useState(false);
    const { refreshContext } = useAppConfig();

    const isLoggedIn = useReactiveVar(isLoggedInVar);
    const inviteToken = useGetInviteTokenFromUrlParams();

    const [acceptRoleMutation] = useAcceptRoleMutation();

    const [isSubmitDisabled, setIsSubmitDisabled] = useState(true);

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

    useEffect(() => {
        if (isLoggedIn && !loading) {
            acceptRole();
            history.push(PageRoutes.ROOT);
        }
    });

    const onFormChange = () => {
        const hasErrors = form.getFieldsError().some(({ errors }) => errors.length > 0);

        const isTouched = form.isFieldsTouched(true);

        setIsSubmitDisabled(hasErrors || !isTouched);
    };

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
                    message.error(`Failed to log in! An unexpected error occurred.`);
                })
                .finally(() => setLoading(false));
        },
        [refreshContext, inviteToken],
    );

    return (
        <Modal
            title={<ModalHeader subHeading="Before we get started we just have a few questions" />}
            buttons={[
                {
                    text: 'Get Started',
                    onClick: () => form.submit(),
                    disabled: isSubmitDisabled,
                    buttonDataTestId: 'sign-up',
                },
            ]}
            onCancel={() => {}}
            mask={false}
            closable={false}
            width="533px"
        >
            <SignupForm
                form={form}
                handleSubmit={handleSignUp}
                onFormChange={onFormChange}
                isSubmitDisabled={isSubmitDisabled}
            />
        </Modal>
    );
}
