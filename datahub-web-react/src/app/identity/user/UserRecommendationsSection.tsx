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
import { formatNumber } from '@app/identity/user/ViewInviteTokenModal.utils';
import { pluralize } from '@app/shared/textUtil';

import { useSendUserInvitationsMutation } from '@graphql/mutations.generated';
import { CorpUser } from '@types';

interface Props {
    recommendedUsers: CorpUser[];
}

export default function UserRecommendationsSection({ recommendedUsers }: Props) {
    const [sendUserInvitationsMutation] = useSendUserInvitationsMutation();

    const handleSendInvitationEmail = async (user: CorpUser) => {
        const displayName = user.properties?.displayName || user.username;
        const email = user.properties?.email || user.info?.email;

        if (!email) {
            message.error(`No email address found for ${displayName}`);
            return;
        }

        try {
            // Show loading state
            const hideLoading = message.loading(`Sending invitation email to ${displayName}...`, 0);

            // Send email using GraphQL mutation
            const result = await sendUserInvitationsMutation({
                variables: {
                    input: {
                        emails: [email],
                        roleUrn: 'urn:li:dataHubRole:DataHubAdmin', // TODO: Make this configurable
                    },
                },
            });

            // Hide loading message
            hideLoading();

            const response = result.data?.sendUserInvitations;
            if (response?.success && response.invitationsSent > 0) {
                message.success(`Invitation email sent to ${displayName} (${email})`);
            } else {
                const errorMessage = response?.errors?.length ? response.errors.join(', ') : 'Unknown error';
                message.error(`Failed to send email: ${errorMessage}`);
            }
        } catch (error) {
            message.error(`Failed to send invitation email to ${displayName}`);
            console.error('Failed to send invitation email:', error);
        }
    };

    console.log('recommendedUsers', recommendedUsers); // TODO: remove
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
                            <Button size="sm" onClick={() => handleSendInvitationEmail(user)}>
                                Send Email
                            </Button>
                        </UserRecommendationCard>
                    );
                })}
            </RecommendationsSection>
        </>
    );
}
