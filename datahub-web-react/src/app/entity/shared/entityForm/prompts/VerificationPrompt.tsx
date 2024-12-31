import React, { useEffect, useRef } from 'react';
import styled from 'styled-components';
import { Button, Divider, message } from 'antd';
import { useVerifyFormMutation } from '../../../../../graphql/form.generated';
import { useEntityContext, useMutationUrn } from '../../EntityContext';
import { PromptWrapper } from './Prompt';

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
}

export default function VerificationPrompt({ formUrn, associatedUrn }: Props) {
    const urn = useMutationUrn();
    const { refetch } = useEntityContext();
    const [verifyFormMutation] = useVerifyFormMutation();

    function verifyForm() {
        verifyFormMutation({ variables: { input: { entityUrn: associatedUrn || urn || '', formUrn } } })
            .then(() => {
                refetch();
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
    }, []);

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
