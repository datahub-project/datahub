import { useReactiveVar } from '@apollo/client';
import { Modal } from '@components';
import { Form, message } from 'antd';
import React, { useCallback, useState } from 'react';
import { Redirect } from 'react-router';

import analytics, { EventType } from '@app/analytics';
import { isLoggedInVar } from '@app/auth/checkAuthStatus';
import ResetCredentialsForm from '@app/auth/resetCredentialsV2/ResetCredentialsForm';
import ModalHeader from '@app/auth/shared/ModalHeader';
import { ResetCredentialsFormValues } from '@app/auth/shared/types';
import useGetResetTokenFromUrlParams from '@app/auth/useGetResetTokenFromUrlParams';
import { Message } from '@app/shared/Message';
import { useAppConfig } from '@app/useAppConfig';
import { PageRoutes } from '@conf/Global';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

export default function ResetCredentialsModal() {
    const [form] = Form.useForm();
    const isLoggedIn = useReactiveVar(isLoggedInVar);
    const resetToken = useGetResetTokenFromUrlParams();

    const [loading, setLoading] = useState(false);
    const [isSubmitDisabled, setIsSubmitDisabled] = useState(true);

    const { refreshContext } = useAppConfig();

    const handleResetCredentials = useCallback(
        (values: ResetCredentialsFormValues) => {
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
            fetch(resolveRuntimePath('/resetNativeUserCredentials'), requestOptions)
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
                    message.error(`Failed to log in!`);
                })
                .finally(() => setLoading(false));
        },
        [refreshContext, resetToken],
    );

    const onFormChange = () => {
        const hasErrors = form.getFieldsError().some(({ errors }) => errors.length > 0);

        const isTouched = form.isFieldsTouched(true);

        setIsSubmitDisabled(hasErrors || !isTouched);
    };

    if (isLoggedIn && !loading) {
        return <Redirect to={`${PageRoutes.ROOT}`} />;
    }

    return (
        <Modal
            title={<ModalHeader />}
            buttons={[
                {
                    text: 'Reset Credentials',
                    onClick: () => form.submit(),
                    disabled: isSubmitDisabled,
                    buttonDataTestId: 'reset-password',
                },
            ]}
            onCancel={() => {}}
            mask={false}
            closable={false}
            width="533px"
        >
            {loading && <Message type="loading" content="Resetting credentials..." />}
            <ResetCredentialsForm
                form={form}
                handleSubmit={handleResetCredentials}
                onFormChange={onFormChange}
                isSubmitDisabled={isSubmitDisabled}
            />
        </Modal>
    );
}
