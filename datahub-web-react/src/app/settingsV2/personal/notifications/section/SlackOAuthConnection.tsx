/**
 * Slack OAuth connection component.
 *
 * Used when requireSlackOAuthBinding = true.
 * Users connect via OAuth flow, similar to Microsoft Teams.
 *
 * The SlackUser is stored via MCP emission from the OAuth callback in slack.py,
 * NOT via GraphQL mutation.
 */
import { Button, colors } from '@components';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import { SLACK_CONNECTION_URN } from '@app/settingsV2/slack/utils';
import { TestNotificationButton } from '@app/shared/notifications/TestNotificationButton';
import { SlackUser } from '@src/types.generated';

import slackLogo from '@images/slacklogo.png';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
    align-items: flex-start;
`;

const ConnectionStatus = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 12px;
`;

const StatusIndicator = styled.div<{ connected: boolean }>`
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background-color: ${(props) => (props.connected ? '#52c41a' : '#d9d9d9')};
`;

const SlackLogo = styled.img`
    height: 16px;
    width: 16px;
    margin-right: 0px;
`;

const HelperText = styled.div`
    color: ${colors.gray[1700]};
    margin-top: 6px;
    font-size: 12px;
`;

const ReconnectLink = styled(Button)`
    background: none;
    border: none;
    padding: 0;
    margin-left: 8px;
    font-size: 12px;
`;

type Props = {
    slackUser: SlackUser | null | undefined;
    onConnect: () => void;
    isConnecting: boolean;
};

/**
 * OAuth-based Slack connection UI.
 * Shows connection status and Connect button.
 */
export const SlackOAuthConnection: React.FC<Props> = ({ slackUser, onConnect, isConnecting }) => {
    const isConnected = !!slackUser?.slackUserId;
    const displayName = slackUser?.displayName || slackUser?.slackUserId;

    return (
        <Container>
            <ConnectionStatus>
                <StatusIndicator connected={isConnected} />
                {isConnected ? (
                    <>
                        <Typography.Text>Connected as</Typography.Text>
                        <SlackLogo src={slackLogo} alt="Slack" />
                        <Typography.Text strong>{displayName}</Typography.Text>
                        <ReconnectLink variant="text" onClick={onConnect} disabled={isConnecting}>
                            Reconnect
                        </ReconnectLink>
                    </>
                ) : (
                    <Typography.Text>Not connected to Slack</Typography.Text>
                )}
            </ConnectionStatus>

            {!isConnected && (
                <Button
                    variant="filled"
                    onClick={onConnect}
                    data-testid="connect-to-slack-oauth-button"
                    disabled={isConnecting}
                >
                    <SlackLogo src={slackLogo} alt="Slack" />
                    Connect to Slack
                </Button>
            )}

            <HelperText>
                {isConnected
                    ? 'You will receive Slack direct messages for entities you are subscribed to & important events.'
                    : 'Connect your Slack account to receive direct message notifications from DataHub.'}
            </HelperText>

            {isConnected && slackUser && (
                <TestNotificationButton
                    integration="slack"
                    connectionUrn={SLACK_CONNECTION_URN}
                    destinationSettings={{
                        user: slackUser,
                    }}
                />
            )}
        </Container>
    );
};
