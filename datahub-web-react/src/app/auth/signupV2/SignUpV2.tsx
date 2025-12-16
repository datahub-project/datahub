import { useReactiveVar } from '@apollo/client';
import { Modal, Text, colors } from '@components';
import { Form, Image, message } from 'antd';
import React, { useCallback, useEffect, useState } from 'react';
import { useHistory } from 'react-router';
import styled, { useTheme } from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { isLoggedInVar } from '@app/auth/checkAuthStatus';
import SignupForm from '@app/auth/signupV2/SignupForm';
import { FormValues } from '@app/auth/signupV2/types';
import useGetInviteTokenFromUrlParams from '@app/auth/useGetInviteTokenFromUrlParams';
import { useAppConfig } from '@app/useAppConfig';
import { PageRoutes } from '@conf/Global';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

import { useAcceptRoleMutation } from '@graphql/mutations.generated';

import backgroundVideo from '@images/signup-animation.mp4';

const HeaderContainer = styled.div`
    display: flex;
    gap: 13px;
    align-items: center;
    padding: 28px 20px 0 20px;
`;

const LogoImage = styled(Image)`
    width: 58px;
    height: auto;
`;

const HeaderText = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

export const VideoWrapper = styled.div`
    position: relative;
    width: 100%;
    height: 100vh;
    overflow: hidden;
    background-color: ${colors.gray[1600]};
`;

const BackgroundVideo = styled.video`
    position: absolute;
    top: 50%;
    left: 50%;
    width: 100vw;
    height: 100vh;
    transform: translate(-50%, -50%);
    z-index: 1;
    object-fit: contain;
`;

export const Content = styled.div`
    position: relative;
    z-index: 2;
    height: 100%;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
`;

export default function SignUpV2() {
    const themeConfig = useTheme();
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
        (values: FormValues) => {
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
        <VideoWrapper>
            <BackgroundVideo src={backgroundVideo} autoPlay muted loop playsInline preload="auto" />
            <Content>
                <Modal
                    title={
                        <HeaderContainer>
                            <LogoImage src={themeConfig.assets?.logoUrl} preview={false} />
                            <HeaderText>
                                <Text size="3xl" color="gray" colorLevel={600} weight="bold" lineHeight="normal">
                                    Welcome to Datahub
                                </Text>
                                <Text size="lg" color="gray" colorLevel={1700} lineHeight="normal">
                                    Before we get started we just have a few questions
                                </Text>
                            </HeaderText>
                        </HeaderContainer>
                    }
                    buttons={[
                        {
                            text: 'Get Started',
                            onClick: () => form.submit(),
                            disabled: isSubmitDisabled,
                        },
                    ]}
                    onCancel={() => {}}
                    mask={false}
                    closable={false}
                    width="533px"
                >
                    <SignupForm form={form} handleSubmit={handleSignUp} onFormChange={onFormChange} />
                </Modal>
            </Content>
        </VideoWrapper>
    );
}
