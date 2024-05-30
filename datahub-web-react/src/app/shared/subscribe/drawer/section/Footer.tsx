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
import { Assertion } from '../../../../../types.generated';

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
    forSubResource?: { assertion?: Assertion };
    onCancelOrUnsubscribe: (isUnsubscribe: boolean) => void;
    onUpdate: () => void;
}

export default function Footer({ isSubscribed, canManageSubscription, forSubResource, onCancelOrUnsubscribe, onUpdate }: Props) {
    const isSlackEnabled = useDrawerSelector(selectIsSlackEnabled);
    const edited = useDrawerSelector(selectIsEdited);
    const hasSlackChannel = useDrawerSelector(selectHasSlackChannel);
    const hasNotificationType = useDrawerSelector(selectHasNotificationType);
    const isSettingsChannelSelection = useDrawerSelector(selectIsSettingsChannelSelection);
    const isSlackFormValid = !isSlackEnabled || hasSlackChannel || isSettingsChannelSelection;
    const canSubmit = edited && isSlackFormValid && canManageSubscription;
    const subscribeText = hasNotificationType ? 'Subscribe & Notify' : 'Subscribe';

    const showUnsubscribe = isSubscribed && !forSubResource;
    return (
        <FooterContainer>
            <FooterButton danger={showUnsubscribe} onClick={() => onCancelOrUnsubscribe(showUnsubscribe)} data-testid="cancel-button">
                {showUnsubscribe ? 'Unsubscribe' : 'Cancel'}
            </FooterButton>
            <FooterButton type="primary" onClick={onUpdate} disabled={!canSubmit} data-testid="subscribe-button">
                {isSubscribed ? 'Update' : subscribeText}
            </FooterButton>
        </FooterContainer>
    );
}
