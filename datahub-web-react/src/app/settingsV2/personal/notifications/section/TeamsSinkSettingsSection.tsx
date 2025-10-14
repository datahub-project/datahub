import { Button, ToggleCard, colors } from '@components';
import { Typography, message } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import { SinkConfigurationContainer } from '@app/settingsV2/personal/notifications/section/styledComponents';
import { TEAMS_CONNECTION_URN, getTeamsConnection } from '@app/settingsV2/teams/utils';
import { createOAuthUrl } from '@app/settingsV2/teams/utils/oauthState';
import { TestNotificationButton } from '@app/shared/notifications/TestNotificationButton';
import { getTeamsSettingsChannelName } from '@app/shared/subscribe/drawer/utils';
import { useAppConfig } from '@app/useAppConfig';
import { useUserContext } from '@src/app/context/useUserContext';
import { getRuntimeBasePath } from '@utils/runtimeBasePath';

import { useConnectionQuery } from '@graphql/connection.generated';
import { useGetTeamsOAuthConfigQuery } from '@graphql/teams.generated';
import { TeamsNotificationSettings, TeamsNotificationSettingsInput } from '@types';

// Remove the hook import since we'll receive the data as a prop

import teamsLogo from '@images/teamslogo.png';

const HelperText = styled.div`
    color: ${colors.gray[1700]};
    margin-top: 6px;
    font-size: 12px;
`;

const CurrentValue = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
    gap: 12px;
    font-size: 14px;
    color: ${colors.gray[1700]};
`;

const OAuthButtonContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
    align-items: flex-start;
`;

const TeamsLogo = styled.img`
    height: 16px;
    width: 16px;
    margin-right: 8px;
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

type Props = {
    isPersonal: boolean;
    sinkSupported: boolean;
    sinkEnabled: boolean;
    updateSinkSetting: (input?: TeamsNotificationSettingsInput) => void;
    toggleSink: (enabled: boolean) => void;
    settings?: TeamsNotificationSettings;
    groupName?: string;
    isTeamsPlatformConfigured: boolean; // Passed down from parent to avoid duplicate queries
};

/**
 * Personal or Group Teams settings section
 */
export const TeamsSinkSettingsSection = ({
    isPersonal,
    sinkSupported, // Whether this sink is supported. If not, the user will not be able to enable it.
    sinkEnabled,
    settings,
    updateSinkSetting,
    toggleSink,
    groupName,
    isTeamsPlatformConfigured, // Received from parent to avoid duplicate queries
}: Props) => {
    const { config } = useAppConfig();
    const me = useUserContext();

    // Check for OAuth success/error parameters
    React.useEffect(() => {
        const urlParams = new URLSearchParams(window.location.search);
        const oauthStatus = urlParams.get('teams_oauth');
        const teamsUserId = urlParams.get('teams_user_id');
        const errorMessage = urlParams.get('message');

        if (oauthStatus === 'success' && teamsUserId) {
            // Refresh the page to load updated settings
            window.location.href = window.location.pathname;
        } else if (oauthStatus === 'error' && errorMessage) {
            message.error(`Teams connection failed: ${errorMessage}`);
        }
    }, []);

    // Keep updateSinkSetting available for future use (e.g., updating user info after OAuth)
    // Currently, connection/disconnection is handled by the toggle switch
    React.useEffect(() => {
        // This ensures updateSinkSetting is referenced to avoid linting errors
        // while keeping it available for future functionality
        if (false) {
            updateSinkSetting({});
        }
    }, [updateSinkSetting]);

    // For personal notifications, we use the TeamsUser structure instead of userHandle
    const teamsUser = isPersonal ? settings?.user : null;
    const displayName = teamsUser?.displayName;
    const userHandle = teamsUser?.email || teamsUser?.azureUserId; // Fallback for display (prefer email over Azure ID)
    const channelName = !isPersonal ? getTeamsSettingsChannelName(isPersonal, settings) : null;

    // Fetch OAuth configuration
    const {
        data: oauthConfigData,
        loading: oauthConfigLoading,
        error: oauthConfigError,
    } = useGetTeamsOAuthConfigQuery();

    // Fetch Teams connection data to get tenant_id
    const { data: connectionData } = useConnectionQuery({
        variables: { urn: TEAMS_CONNECTION_URN },
        errorPolicy: 'ignore',
        fetchPolicy: 'cache-first',
    });
    const teamsConnection = getTeamsConnection(connectionData);
    const tenantId = teamsConnection?.tenant_id;

    // Teams platform configuration is passed down as prop (queried once in parent)

    // Check if user is connected to Teams (has userHandle for personal, or channels for group)
    const isConnected = isPersonal ? !!userHandle : !!channelName;

    const isTeamsEnabled = config?.featureFlags?.teamsNotificationsEnabled || false;

    // Don't render Teams section if feature flag is disabled
    if (!isTeamsEnabled) {
        return null;
    }

    // Override the platform configuration check with actual connection data
    // If Teams connection exists with tenant_id, platform integration is configured
    const actualPlatformNotConfigured = isTeamsEnabled && !isTeamsPlatformConfigured;

    // Show Teams section if feature is enabled, regardless of platform configuration
    // This allows us to show setup guidance when platform is not configured

    const isAdminAccess = me?.platformPrivileges?.manageGlobalSettings || false;

    // Generate OAuth URL for personal Teams connection
    const generateOAuthUrl = () => {
        const oauthConfig = oauthConfigData?.teamsOAuthConfig;
        if (!oauthConfig) {
            return '';
        }

        // Get current DataHub instance URL
        const instanceUrl = window.location.origin + getRuntimeBasePath();

        // Use shared utility to create OAuth URL for personal notifications
        return createOAuthUrl(
            {
                url: instanceUrl,
                flowType: 'personal_notifications', // ← KEY: Tells router this is user binding, not platform setup
                ...(tenantId && { tenantId }), // ← CRITICAL: Include tenant_id only if present
                userUrn: me?.urn,
                redirectPath: '/settings/personal-notifications', // Where to redirect after completion
            },
            {
                appId: oauthConfig.appId,
                redirectUri: oauthConfig.redirectUri, // This will go to the multi-tenant router
                scopes: oauthConfig.scopes,
                baseAuthUrl: oauthConfig.baseAuthUrl,
            },
            'common', // Use common endpoint for tenant-agnostic OAuth flow
        );
    };

    const startOAuthFlow = () => {
        if (oauthConfigLoading) {
            message.info('Loading OAuth configuration...');
            return;
        }

        if (oauthConfigError || !oauthConfigData?.teamsOAuthConfig) {
            message.error('Failed to load OAuth configuration. Please try again.');
            return;
        }

        if (!tenantId) {
            message.error('Teams platform integration is not properly configured. Please contact your administrator.');
            return;
        }

        const oauthUrl = generateOAuthUrl();
        if (!oauthUrl) {
            message.error('Failed to generate OAuth URL. Please check the configuration.');
            return;
        }

        message.info('Redirecting to Microsoft for Teams authentication...');

        // Redirect to OAuth flow
        window.location.href = oauthUrl;
    };

    const renderSinkDescription = () => {
        const actorDescription = isPersonal ? 'you are' : `${groupName || 'the group'} is`;
        const supportedSinkDescription = `Receive Teams notifications for entities ${actorDescription} subscribed to & important events.`;

        // Handle the case where feature is enabled but platform integration is not set up
        if (actualPlatformNotConfigured) {
            if (isAdminAccess) {
                return (
                    <>
                        Teams notifications are available, but the platform integration is not configured.&nbsp;
                        <Link to="/settings/integrations/microsoft-teams" style={{ color: colors.violet['500'] }}>
                            Click here to set up Teams integration
                        </Link>{' '}
                        in Platform Settings.
                    </>
                );
            }
            return (
                <>
                    Teams notifications are available, but your admin needs to configure the platform integration first.
                    Ask your DataHub admin to set up Teams integration in Platform Settings.
                </>
            );
        }

        // Handle normal cases: supported or completely disabled
        if (!sinkSupported && isAdminAccess) {
            return (
                <>
                    Teams is currently disabled.&nbsp;
                    <Link to="/settings/integrations/microsoft-teams" style={{ color: colors.violet['500'] }}>
                        Click here
                    </Link>{' '}
                    to setup the Teams integration.
                </>
            );
        }

        if (!sinkSupported && !isAdminAccess) {
            return <>In order to enable, ask your DataHub admin to setup the Teams integration.</>;
        }

        return <>{supportedSinkDescription}</>;
    };

    return (
        <ToggleCard
            title="Microsoft Teams Notifications"
            subTitle={renderSinkDescription()}
            disabled={!sinkSupported || actualPlatformNotConfigured}
            value={sinkEnabled}
            onToggle={toggleSink}
            toggleDataTestId="teams-notifications-enabled-switch"
        >
            {sinkEnabled && (
                <SinkConfigurationContainer>
                    {isPersonal ? (
                        <OAuthButtonContainer>
                            <ConnectionStatus>
                                <StatusIndicator connected={isConnected} />
                                {isConnected ? (
                                    <>
                                        <Typography.Text>Connected as:</Typography.Text>
                                        <TeamsLogo src={teamsLogo} alt="Teams" />
                                        <Typography.Text strong>{displayName || userHandle}</Typography.Text>
                                    </>
                                ) : (
                                    <Typography.Text>Not connected to Teams</Typography.Text>
                                )}
                            </ConnectionStatus>

                            {!isConnected && (
                                <Button
                                    variant="filled"
                                    onClick={startOAuthFlow}
                                    data-testid="connect-to-teams-button"
                                    disabled={oauthConfigLoading}
                                >
                                    <TeamsLogo src={teamsLogo} alt="Teams" />
                                    Connect to Teams
                                </Button>
                            )}

                            <HelperText>
                                {isConnected ? (
                                    <>
                                        You will receive Teams direct messages for entities you are subscribed to &
                                        important events.
                                    </>
                                ) : (
                                    <>
                                        Connect your Teams account to receive direct message notifications from DataHub.
                                    </>
                                )}
                            </HelperText>

                            {isConnected && (
                                <TestNotificationButton
                                    integration="teams"
                                    connectionUrn={TEAMS_CONNECTION_URN}
                                    destinationSettings={{
                                        user: {
                                            teamsUserId: teamsUser?.teamsUserId || undefined,
                                            azureUserId: teamsUser?.azureUserId || undefined,
                                            email: teamsUser?.email || undefined,
                                            displayName: teamsUser?.displayName || undefined,
                                            lastUpdated: teamsUser?.lastUpdated || undefined,
                                        },
                                    }}
                                />
                            )}
                        </OAuthButtonContainer>
                    ) : (
                        <CurrentValue>
                            <Typography.Text type="secondary">
                                Group Teams notifications are not supported yet. Please use the platform-level Teams
                                integration.
                            </Typography.Text>
                        </CurrentValue>
                    )}
                </SinkConfigurationContainer>
            )}
        </ToggleCard>
    );
};
