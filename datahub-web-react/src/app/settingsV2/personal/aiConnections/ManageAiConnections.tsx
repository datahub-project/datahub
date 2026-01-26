import { LinkOutlined, SettingOutlined } from '@ant-design/icons';
import { Empty, Spin, message } from 'antd';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import AiConnectionCard from '@app/settingsV2/personal/aiConnections/AiConnectionCard';
import ApiKeyModal from '@app/settingsV2/personal/aiConnections/ApiKeyModal';
import { useOAuthConnect } from '@app/settingsV2/personal/aiConnections/useOAuthConnect';
import { useGetAuthenticatedUser } from '@app/useGetAuthenticatedUser';
import { colors } from '@src/alchemy-components';

import {
    useGetAiPluginsWithUserStatusQuery,
    useUpdateUserAiPluginSettingsMutation,
} from '@graphql/aiPlugins.generated';
import { AiPluginAuthType } from '@types';

const Container = styled.div`
    padding: 24px;
    max-width: 800px;
`;

const Header = styled.div`
    margin-bottom: 24px;
`;

const Title = styled.h1`
    font-size: 20px;
    font-weight: 700;
    color: ${colors.gray[600]};
    margin: 0 0 8px 0;
    display: flex;
    align-items: center;
    gap: 8px;
`;

const Description = styled.p`
    font-size: 14px;
    color: ${colors.gray[1700]};
    margin: 0;
    line-height: 1.5;
`;

const CardGrid = styled.div`
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
    gap: 16px;
`;

const StyledCard = styled.div`
    border: 1px solid ${colors.gray[100]};
    border-radius: 12px;
    box-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07);
    padding: 24px;
`;

/** Plugin info needed for the API key modal */
interface ApiKeyModalState {
    pluginId: string;
    pluginName: string;
}

/**
 * Helper to check if a plugin requires user connection (OAuth or API key).
 */
function requiresUserConnection(authType: AiPluginAuthType): boolean {
    return authType === AiPluginAuthType.UserOauth || authType === AiPluginAuthType.UserApiKey;
}

/**
 * User-facing settings page for managing AI plugin preferences.
 *
 * Users can:
 * - Connect their personal accounts (OAuth or API key) for plugins that require it
 * - Enable/disable any plugin for their personal use
 *
 * Plugin types:
 * - USER_OAUTH / USER_API_KEY: Require connection, enabled by default after connection
 * - SHARED_API_KEY / NONE: No connection needed, disabled by default until user enables
 *
 * Note: Only plugins enabled by admin (in global settings) are shown here.
 */
export const ManageAiConnections: React.FC = () => {
    const { data, loading, refetch } = useGetAiPluginsWithUserStatusQuery();
    const [updateUserSettings] = useUpdateUserAiPluginSettingsMutation();
    const { initiateOAuthConnect, isConnecting } = useOAuthConnect(refetch);
    const [togglingPluginId, setTogglingPluginId] = useState<string | null>(null);
    const [apiKeyModal, setApiKeyModal] = useState<ApiKeyModalState | null>(null);
    const authenticatedUser = useGetAuthenticatedUser();

    // Check if the current user is the "admin" user (for showing debug features)
    const isAdminUser = authenticatedUser?.corpUser?.username === 'admin';

    // Get all enabled plugins with user status
    const availablePlugins = useMemo(() => {
        const globalPlugins = data?.globalSettings?.aiPlugins || [];
        const userPlugins = data?.me?.corpUser?.settings?.aiPluginSettings?.plugins || [];

        // Filter to only plugins that are enabled by admin
        return globalPlugins
            .filter((plugin) => plugin.enabled)
            .map((plugin) => {
                // Find the user's settings for this plugin
                const userConfig = userPlugins.find((up) => up.id === plugin.id);
                const needsConnection = requiresUserConnection(plugin.authType);

                // Determine if user has connected (only relevant for USER_OAUTH/USER_API_KEY)
                let isConnected: boolean;
                if (!needsConnection) {
                    // SHARED_API_KEY/NONE don't need connection
                    isConnected = true;
                } else if (plugin.authType === AiPluginAuthType.UserOauth) {
                    isConnected = userConfig?.oauthConfig?.isConnected || false;
                } else {
                    isConnected = userConfig?.apiKeyConfig?.isConnected || false;
                }

                // Determine enabled state - require explicit enabled=true
                // The backend sets enabled=true when OAuth connection completes,
                // so we don't need to infer enabled from connected status.
                const isEnabled = userConfig?.enabled === true;

                return {
                    ...plugin,
                    userConfig,
                    isConnected,
                    isEnabled,
                };
            });
    }, [data]);

    // Handle URL fragment to highlight a specific plugin card (e.g., #plugin-id)
    useEffect(() => {
        if (!loading && availablePlugins.length > 0) {
            const hash = window.location.hash.slice(1); // Remove #
            if (hash) {
                // Delay slightly to ensure DOM is rendered
                const timeoutId = setTimeout(() => {
                    const element = document.getElementById(`plugin-${hash}`);
                    if (element) {
                        element.scrollIntoView({ behavior: 'smooth', block: 'center' });
                        element.classList.add('highlighted');
                        // Remove highlight after 3 seconds
                        setTimeout(() => {
                            element.classList.remove('highlighted');
                        }, 3000);
                    }
                }, 100);
                return () => clearTimeout(timeoutId);
            }
        }
        return undefined;
    }, [loading, availablePlugins]);

    const handleConnect = useCallback(
        async (pluginId: string, pluginName: string, authType: AiPluginAuthType) => {
            if (authType === AiPluginAuthType.UserOauth) {
                await initiateOAuthConnect(pluginId);
            } else {
                // Open API key modal
                setApiKeyModal({ pluginId, pluginName });
            }
        },
        [initiateOAuthConnect],
    );

    const handleApiKeySubmit = useCallback(
        async (apiKey: string) => {
            if (!apiKeyModal) return;

            try {
                // Call the backend API endpoint to save the API key
                // This endpoint saves to DataHubConnection and updates CorpUserSettings
                const response = await fetch(
                    `/integrations/oauth/plugins/${encodeURIComponent(apiKeyModal.pluginId)}/api-key`,
                    {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                        },
                        credentials: 'include',
                        body: JSON.stringify({ api_key: apiKey }),
                    },
                );

                if (!response.ok) {
                    const errorData = await response.json().catch(() => ({}));
                    throw new Error(errorData.detail || `Failed to save API key: ${response.status}`);
                }

                message.success(`Connected to ${apiKeyModal.pluginName} successfully!`);
                refetch();
            } catch (error) {
                message.error(error instanceof Error ? error.message : 'Failed to save API key. Please try again.');
                throw error; // Re-throw so the modal knows submission failed
            }
        },
        [apiKeyModal, refetch],
    );

    const handleToggleEnabled = useCallback(
        async (pluginId: string, enabled: boolean) => {
            setTogglingPluginId(pluginId);
            try {
                await updateUserSettings({
                    variables: {
                        input: {
                            pluginId,
                            enabled,
                        },
                    },
                });
                message.success(enabled ? 'Plugin enabled' : 'Plugin disabled');
                refetch();
            } catch (error) {
                message.error('Failed to update plugin. Please try again.');
            } finally {
                setTogglingPluginId(null);
            }
        },
        [updateUserSettings, refetch],
    );

    const handleDisconnect = useCallback(
        async (pluginId: string, pluginName: string) => {
            try {
                const response = await fetch(`/integrations/oauth/plugins/${encodeURIComponent(pluginId)}/disconnect`, {
                    method: 'DELETE',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    credentials: 'include',
                });

                if (!response.ok) {
                    const errorData = await response.json().catch(() => ({}));
                    throw new Error(errorData.detail || `Failed to disconnect: ${response.status}`);
                }

                message.success(`Disconnected from ${pluginName}`);
                refetch();
            } catch (error) {
                message.error(error instanceof Error ? error.message : 'Failed to disconnect. Please try again.');
            }
        },
        [refetch],
    );

    /**
     * Admin-only handler to corrupt OAuth credentials for testing auth error flows.
     * This will cause the next API call to fail with a 401, triggering the auto-disconnect flow.
     */
    const handleCorruptCredentials = useCallback(async (pluginId: string, pluginName: string) => {
        try {
            const response = await fetch(
                `/integrations/oauth/plugins/${encodeURIComponent(pluginId)}/corrupt-credentials`,
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    credentials: 'include',
                },
            );

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({}));
                throw new Error(errorData.detail || `Failed to corrupt credentials: ${response.status}`);
            }

            message.warning(
                `Corrupted credentials for ${pluginName}. The next chat request using this plugin will trigger an auth error.`,
            );
        } catch (error) {
            message.error(error instanceof Error ? error.message : 'Failed to corrupt credentials. Please try again.');
        }
    }, []);

    if (loading) {
        return (
            <Container>
                <Spin style={{ marginTop: 40 }} />
            </Container>
        );
    }

    return (
        <Container>
            <Header>
                <Title>
                    <LinkOutlined />
                    AI Plugins
                </Title>
                <Description>
                    Manage your AI plugin preferences for Ask DataHub. Enable plugins to use them in chat, or connect
                    your personal accounts for plugins that require authentication.
                </Description>
            </Header>

            <StyledCard>
                <Title style={{ fontSize: '16px', marginBottom: '16px' }}>
                    <SettingOutlined />
                    Available Plugins
                </Title>

                {availablePlugins.length === 0 ? (
                    <Empty
                        description={
                            <span>
                                No AI plugins are configured.
                                <br />
                                Contact your administrator to set up AI plugins.
                            </span>
                        }
                    />
                ) : (
                    <CardGrid>
                        {availablePlugins.map((plugin) => (
                            <AiConnectionCard
                                key={plugin.id}
                                plugin={plugin}
                                isConnected={plugin.isConnected}
                                isEnabled={plugin.isEnabled}
                                onConnect={() =>
                                    handleConnect(
                                        plugin.id,
                                        plugin.service?.properties?.displayName || 'Unknown Plugin',
                                        plugin.authType,
                                    )
                                }
                                onToggleEnabled={(enabled) => handleToggleEnabled(plugin.id, enabled)}
                                onDisconnect={() =>
                                    handleDisconnect(
                                        plugin.id,
                                        plugin.service?.properties?.displayName || 'Unknown Plugin',
                                    )
                                }
                                onCorruptCredentials={() =>
                                    handleCorruptCredentials(
                                        plugin.id,
                                        plugin.service?.properties?.displayName || 'Unknown Plugin',
                                    )
                                }
                                isConnecting={isConnecting}
                                isToggling={togglingPluginId === plugin.id}
                                isAdminUser={isAdminUser}
                            />
                        ))}
                    </CardGrid>
                )}
            </StyledCard>

            {/* API Key Modal */}
            <ApiKeyModal
                open={apiKeyModal !== null}
                pluginName={apiKeyModal?.pluginName || ''}
                onClose={() => setApiKeyModal(null)}
                onSubmit={handleApiKeySubmit}
            />
        </Container>
    );
};

export default ManageAiConnections;
