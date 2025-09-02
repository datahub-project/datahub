import { Button, Text } from '@components';
import React from 'react';

import {
    MetricBadge,
    UserRecommendationCard as StyledCard,
    UserInfo,
    UserMetrics,
    UserName,
} from '@app/identity/user/ViewInviteTokenModal.components';
import { formatNumber } from '@app/shared/formatNumber';

import { CorpUser } from '@types';

interface Props {
    user: CorpUser;
    onSendInvitation: (user: CorpUser) => void;
}

/**
 * Component responsible for displaying a single user recommendation
 * Handles only UI presentation logic
 */
export default function UserRecommendationCard({ user, onSendInvitation }: Props) {
    const displayName = user.properties?.displayName || user.username;
    const title = user.properties?.title;
    const queryCount = user.usageFeatures?.userUsageTotalPast30Days || 0;
    const platformCount = user.usageFeatures?.userPlatformUsageTotalsPast30Days?.length || 0;

    return (
        <StyledCard>
            <UserInfo>
                <UserName>{displayName}</UserName>
                {title && (
                    <Text color="gray" size="sm" style={{ fontSize: 12 }}>
                        {title}
                    </Text>
                )}
                <UserMetrics>
                    <MetricBadge>{formatNumber(queryCount)} queries</MetricBadge>
                    <MetricBadge>{platformCount} platforms</MetricBadge>
                </UserMetrics>
            </UserInfo>
            <Button size="sm" onClick={() => onSendInvitation(user)}>
                Send Email
            </Button>
        </StyledCard>
    );
}
