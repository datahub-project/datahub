/**
 * SlackOAuthUserDisplay - Shows the OAuth-connected Slack user's display name.
 *
 * Used in SubscriptionDrawer when requireSlackOAuthBinding = true.
 * Similar pattern to TeamsSearchInterface.tsx.
 *
 * If user is OAuth-connected: Shows "Notifications will be sent to: <name>"
 * If user is NOT connected: Shows message to connect in settings
 */
import { Typography } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import { SLACK_CONNECTION_URN } from '@app/settingsV2/slack/utils';
import { TestNotificationButton } from '@src/app/shared/notifications/TestNotificationButton';

import { useGetUserNotificationSettingsQuery } from '@graphql/settings.generated';

import slackLogo from '@images/slacklogo.png';

const LEFT_PADDING = 36;

const StyledSlackSection = styled.div`
    padding-left: ${LEFT_PADDING}px;
    margin-top: 8px;
`;

const PersonalSlackInfo = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 12px;
    background: #f6f7f9;
    border-radius: 6px;
    margin-bottom: 12px;
`;

const SlackIcon = styled.img`
    height: 16px;
    width: 16px;
`;

const SetupMessage = styled.div`
    padding: 12px;
    background: #fffbe6;
    border: 1px solid #ffe58f;
    border-radius: 6px;
    margin-bottom: 12px;
`;

const TestNotificationButtonWrapper = styled.div`
    margin-top: 8px;
`;

type SlackOAuthUserDisplayProps = {
    slackSinkSupported: boolean;
};

/**
 * Displays the OAuth-connected Slack user or prompts to connect.
 * For use in SubscriptionDrawer when requireSlackOAuthBinding = true.
 */
export default function SlackOAuthUserDisplay({ slackSinkSupported }: SlackOAuthUserDisplayProps) {
    // Get user's personal notification settings to check if Slack is OAuth-configured
    const { data: userNotificationSettings } = useGetUserNotificationSettingsQuery();
    const slackSettings = userNotificationSettings?.getUserNotificationSettings?.slackSettings;

    // Check for OAuth-bound SlackUser (new way)
    const slackUser = slackSettings?.user;
    const isSlackOAuthConfigured = !!slackUser?.slackUserId;

    // Display name from OAuth binding
    const displayName = slackUser?.displayName || slackUser?.slackUserId;

    if (!slackSinkSupported) {
        return null; // Handled by parent component
    }

    // User has NOT connected via OAuth - show setup message
    if (!isSlackOAuthConfigured) {
        return (
            <StyledSlackSection>
                <SetupMessage>
                    <Typography.Text>
                        Before you can subscribe, you need to{' '}
                        <Link to="/settings/personal-notifications" style={{ color: '#1890ff' }}>
                            connect your Slack account
                        </Link>
                        .
                    </Typography.Text>
                </SetupMessage>
            </StyledSlackSection>
        );
    }

    // User IS OAuth-connected - show their name
    return (
        <StyledSlackSection>
            <PersonalSlackInfo>
                <SlackIcon src={slackLogo} alt="Slack" />
                <Typography.Text>
                    Notifications will be sent to <strong>{displayName}</strong>
                </Typography.Text>
            </PersonalSlackInfo>
            <TestNotificationButtonWrapper>
                <TestNotificationButton
                    integration="slack"
                    connectionUrn={SLACK_CONNECTION_URN}
                    destinationSettings={{
                        user: slackUser,
                    }}
                />
            </TestNotificationButtonWrapper>
        </StyledSlackSection>
    );
}
