import { useReactiveVar } from '@apollo/client';
import { Modal } from '@components';
import { Form, message } from 'antd';
import React, { useCallback, useEffect, useState } from 'react';
import { useHistory } from 'react-router';

import analytics, { EventType } from '@app/analytics';
import { isLoggedInVar } from '@app/auth/checkAuthStatus';
import { USER_SIGNED_UP_KEY } from '@app/auth/constants';
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

    const acceptRole = async () => {
        const { errors } = await acceptRoleMutation({
            variables: {
                input: {
                    inviteToken,
                },
            },
            // Refetch the user's data to ensure the new role is reflected
            refetchQueries: ['getMe'],
            awaitRefetchQueries: true,
        });

        if (!errors) {
            message.success({
                content: `Accepted invite!`,
                duration: 2,
            });
        }
    };

    useEffect(() => {
        if (isLoggedIn && !loading && inviteToken) {
            // Set flag to trigger permission polling after signup
            localStorage.setItem(USER_SIGNED_UP_KEY, Date.now().toString());

            acceptRole()
                .then(() => {
                    history.push(PageRoutes.ROOT);
                })
                .catch((error) => {
                    console.error('Failed to accept role:', error);
                    const errorMessage = error instanceof Error ? error.message : '';
                    message.error({
                        content: `Failed to accept invite: \n ${errorMessage}`,
                        duration: 3,
                    });
                    // Still redirect even if role acceptance fails, as user is already signed up
                    history.push(PageRoutes.ROOT);
                });
        } else if (isLoggedIn && !loading && !inviteToken) {
            // No invite token, just redirect
            history.push(PageRoutes.ROOT);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [isLoggedIn, loading]);

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
