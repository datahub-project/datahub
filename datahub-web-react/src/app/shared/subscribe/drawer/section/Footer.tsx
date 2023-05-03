import React from 'react';
import styled from 'styled-components/macro';
import { Button, Typography } from 'antd';

const FooterContainer = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: flex-end;
    margin-top: 16px;
    margin-right: 24px;
    margin-bottom: 16px;
    gap: 8px;
`;

const FooterButton = styled(Button)`
    display: inline-flex;
    align-items: center;
`;

const FooterButtonLabel = styled(Typography.Text)<{ color: string }>`
    font-family: 'Manrope', sans-serif;
    font-size: 14px;
    font-weight: 400;
    color: ${({ color }) => color};
`;

interface Props {
    isSubscribed: boolean;
    allowEditing: boolean;
    onCancelOrUnsubscribe: () => void;
    onUpdate: () => void;
}

export default function Footer({ isSubscribed, allowEditing, onCancelOrUnsubscribe, onUpdate }: Props) {
    const leftButtonText: string = isSubscribed ? 'Unsubscribe' : 'Cancel';
    const leftButtonColor: string = isSubscribed ? '#FF4D4F' : 'rgba(0, 0, 0, 0.88)';
    const subscribeButtonText: string = isSubscribed ? 'Update' : 'Subscribe';
    const allowSubscribe: boolean = isSubscribed || allowEditing;

    return (
        <FooterContainer>
            <FooterButton onClick={onCancelOrUnsubscribe}>
                <FooterButtonLabel color={leftButtonColor}>{leftButtonText}</FooterButtonLabel>
            </FooterButton>
            <FooterButton type="primary" onClick={onUpdate} disabled={!allowSubscribe}>
                <FooterButtonLabel color="white">{subscribeButtonText}</FooterButtonLabel>
            </FooterButton>
        </FooterContainer>
    );
}
