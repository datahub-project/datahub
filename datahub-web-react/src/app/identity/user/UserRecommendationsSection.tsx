import { Button, Text } from '@components';
import { Divider, message } from 'antd';
import React from 'react';

import {
    MetricBadge,
    RecommendationsSection,
    UserInfo,
    UserMetrics,
    UserName,
    UserRecommendationCard,
} from '@app/identity/user/ViewInviteTokenModal.components';
import { createUserInviteLink, formatNumber } from '@app/identity/user/ViewInviteTokenModal.utils';
import { pluralize } from '@app/shared/textUtil';

import { CorpUser } from '@types';

interface Props {
    recommendedUsers: CorpUser[];
    inviteLink: string;
}

export default function UserRecommendationsSection({ recommendedUsers, inviteLink }: Props) {
    const handleCopyInviteLink = (user: CorpUser) => {
        const displayName = user.properties?.displayName || user.username;
        const userInviteLink = createUserInviteLink(inviteLink, user.username);
        navigator.clipboard.writeText(userInviteLink);
        message.success(`Copied invite link for ${displayName}`);
    };

    if (recommendedUsers.length === 0) {
        return null;
    }

    return (
        <>
            <Divider />
            <Text size="lg" weight="semiBold">
                Recommended Users
            </Text>
            <Text color="gray" size="md" style={{ marginBottom: 12 }}>
                {recommendedUsers.length} {pluralize(recommendedUsers.length, 'user')} had platform activity in the last
                30 days.
            </Text>
            <RecommendationsSection>
                {recommendedUsers.map((user) => {
                    const displayName = user.properties?.displayName || user.username;
                    const title = user.properties?.title;
                    const queryCount = user.usageFeatures?.userUsageTotalPast30Days || 0;
                    const platformCount = user.usageFeatures?.userPlatformUsageTotalsPast30Days?.length || 0;

                    return (
                        <UserRecommendationCard key={user.urn}>
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
                            <Button size="sm" onClick={() => handleCopyInviteLink(user)}>
                                Copy Link
                            </Button>
                        </UserRecommendationCard>
                    );
                })}
            </RecommendationsSection>
        </>
    );
}
