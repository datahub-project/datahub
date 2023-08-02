import React from 'react';
import styled from 'styled-components/macro';
import { Button } from 'antd';
import { useDrawerState } from '../state/context';
import { ChannelSelections } from '../state/types';

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
    const { edited, slack } = useDrawerState();
    const hasSlackChannel = slack.channelSelection === ChannelSelections.SUBSCRIPTION && slack.subscription.channel;
    const isSlackFormValid = !slack.enabled || hasSlackChannel;
    const canSubmit = edited && isSlackFormValid;

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
