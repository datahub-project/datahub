import { Button, Divider, message } from 'antd';
import React, { useEffect, useRef } from 'react';
import styled from 'styled-components';

import analytics, { DocRequestView, EventType } from '@app/analytics';
import { useEntityContext, useMutationUrn } from '@app/entity/shared/EntityContext';
import { PromptWrapper } from '@app/entity/shared/entityForm/prompts/Prompt';
import { FORM_ASSET_COMPLETION } from '@app/onboarding/config/FormOnboardingConfig';
import { useUpdateEducationStepsAllowList } from '@app/onboarding/useUpdateEducationStepsAllowList';

import { useVerifyFormMutation } from '@graphql/form.generated';

const ContentWrapper = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    font-size: 16px;
    font-weight: 600;
`;

const VerifyButton = styled(Button)`
    margin-top: 16px;
    width: 60%;
    max-width: 600px;
    font-size: 16px;
    font-weight: 600;
    height: auto;
`;

interface Props {
    formUrn: string;
    associatedUrn?: string;
    shouldShowVerificationPrompt?: boolean;
}

export default function VerificationPrompt({ formUrn, associatedUrn, shouldShowVerificationPrompt = false }: Props) {
    const urn = useMutationUrn();
    const { refetch } = useEntityContext();
    const [verifyFormMutation] = useVerifyFormMutation();
    const { addIdToAllowList } = useUpdateEducationStepsAllowList();

    function verifyForm() {
        verifyFormMutation({ variables: { input: { entityUrn: associatedUrn || urn || '', formUrn } } })
            .then(() => {
                refetch();
                addIdToAllowList(FORM_ASSET_COMPLETION);
                analytics.event({
                    type: EventType.CompleteVerification,
                    source: DocRequestView.ByAsset,
                    numAssets: 1,
                });
            })
            .catch(() => {
                message.error('Error when verifying responses on form');
            });
    }

    const verificationPrompt = useRef(null);
    useEffect(() => {
        (verificationPrompt?.current as any)?.scrollIntoView({
            behavior: 'smooth',
            block: 'start',
            inline: 'nearest',
        });
    }, [shouldShowVerificationPrompt]);

    return (
        <>
            <Divider />
            <PromptWrapper ref={verificationPrompt}>
                <ContentWrapper>
                    <span>All questions for verification have been completed. Please verify your responses.</span>
                    <VerifyButton type="primary" onClick={verifyForm}>
                        Verify Responses
                    </VerifyButton>
                </ContentWrapper>
            </PromptWrapper>
        </>
    );
}
