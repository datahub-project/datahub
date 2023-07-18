import React from 'react';
import styled from 'styled-components/macro';
import { Button } from 'antd';
import { useFormStateContext } from '../form/context';

const FooterContainer = styled.div`
    display: flex;
    justify-content: flex-end;
    margin-top: 16px;
    margin-right: 24px;
    margin-bottom: 16px;
    gap: 8px;
`;

const FooterButton = styled(Button)``;

interface Props {
    isSubscribed: boolean;
    onCancelOrUnsubscribe: () => void;
    onUpdate: () => void;
}

export default function Footer({ isSubscribed, onCancelOrUnsubscribe, onUpdate }: Props) {
    const { edited, slack } = useFormStateContext();
    const canSubmit = edited && (isSubscribed || slack.enabled);

    return (
        <FooterContainer>
            <FooterButton danger={isSubscribed} onClick={onCancelOrUnsubscribe}>
                {isSubscribed ? 'Unsubscribe' : 'Cancel'}
            </FooterButton>
            <FooterButton type="primary" onClick={onUpdate} disabled={!canSubmit}>
                {isSubscribed ? 'Update' : 'Subscribe'}
            </FooterButton>
        </FooterContainer>
    );
}
