import React from 'react';
import styled from 'styled-components/macro';
import { Button } from 'antd';
import {
    selectHasNotificationType,
    selectHasSlackChannel,
    selectIsEdited,
    selectIsSettingsChannelSelection,
    selectIsSlackEnabled,
    useDrawerSelector,
} from '../state/selectors';

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
    canManageSubscription?: boolean | null;
    isSubscribed: boolean;
    onCancelOrUnsubscribe: () => void;
    onUpdate: () => void;
}

export default function Footer({ isSubscribed, canManageSubscription, onCancelOrUnsubscribe, onUpdate }: Props) {
    const isSlackEnabled = useDrawerSelector(selectIsSlackEnabled);
    const edited = useDrawerSelector(selectIsEdited);
    const hasSlackChannel = useDrawerSelector(selectHasSlackChannel);
    const hasNotificationType = useDrawerSelector(selectHasNotificationType);
    const isSettingsChannelSelection = useDrawerSelector(selectIsSettingsChannelSelection);
    const isSlackFormValid = !isSlackEnabled || hasSlackChannel || isSettingsChannelSelection;
    const canSubmit = edited && isSlackFormValid && canManageSubscription;
    const subscribeText = hasNotificationType ? 'Subscribe & Notify' : 'Subscribe';

    return (
        <FooterContainer>
            <FooterButton danger={isSubscribed} onClick={onCancelOrUnsubscribe} data-testid="cancel-button">
                {isSubscribed ? 'Unsubscribe' : 'Cancel'}
            </FooterButton>
            <FooterButton type="primary" onClick={onUpdate} disabled={!canSubmit} data-testid="subscribe-button">
                {isSubscribed ? 'Update' : subscribeText}
            </FooterButton>
        </FooterContainer>
    );
}
