import { Empty, Spin, message } from 'antd';
import React, { useCallback, useMemo, useState } from 'react';
import styled from 'styled-components';

import ApiKeyModal, { AdditionalApiKeyField } from '@app/settingsV2/personal/aiConnections/ApiKeyModal';
import CustomHeadersModal from '@app/settingsV2/personal/aiConnections/CustomHeadersModal';
import { useOAuthConnect } from '@app/settingsV2/personal/aiConnections/useOAuthConnect';
import IntegrationCard from '@app/settingsV2/personal/myIntegrations/IntegrationCard';
import { mergeCustomHeaders } from '@app/settingsV2/personal/myIntegrations/utils/headerMergeUtils';
import { mergePluginsWithUserConfig } from '@app/settingsV2/personal/myIntegrations/utils/pluginDataMapper';
import { detectPluginSourceName, getPluginSource } from '@app/settingsV2/platform/ai/plugins/sources/pluginSources';
import { PageTitle } from '@src/alchemy-components';

import {
    useGetAiPluginsWithUserStatusQuery,
    useUpdateUserAiPluginSettingsMutation,
} from '@graphql/aiPlugins.generated';
import { AiPluginAuthType } from '@types';

const Container = styled.div`
    width: 100%;
    overflow: auto;
    padding: 16px 20px;
`;

const PluginsList = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
    margin-top: 24px;
`;

interface ApiKeyModalState {
    pluginId: string;
    pluginName: string;
    pluginUrl?: string | null;
}

interface CustomHeadersModalState {
    pluginId: string;
    pluginName: string;
}

export const ManageMyAiSettings: React.FC = () => {
    const { data, loading, refetch } = useGetAiPluginsWithUserStatusQuery({
        fetchPolicy: 'cache-and-network',
    });

    const [updateUserSettings] = useUpdateUserAiPluginSettingsMutation();
    const { initiateOAuthConnect, connectingPluginId } = useOAuthConnect(refetch);
    const [togglingPluginId, setTogglingPluginId] = useState<string | null>(null);
    const [apiKeyModal, setApiKeyModal] = useState<ApiKeyModalState | null>(null);
    const [customHeadersModal, setCustomHeadersModal] = useState<CustomHeadersModalState | null>(null);
    const availablePlugins = useMemo(() => {
        const globalPlugins = (data?.globalSettings?.aiPlugins || []) as Parameters<
            typeof mergePluginsWithUserConfig
        >[0];
        const userPlugins = (data?.me?.corpUser?.settings?.aiPluginSettings?.plugins || []) as Parameters<
            typeof mergePluginsWithUserConfig
        >[1];

        return mergePluginsWithUserConfig(globalPlugins, userPlugins);
    }, [data]);

    // Derive additional fields for the API key modal based on the plugin source
    const apiKeyAdditionalFields = useMemo((): AdditionalApiKeyField[] => {
        if (!apiKeyModal) return [];

        const sourceName = detectPluginSourceName(apiKeyModal.pluginUrl, apiKeyModal.pluginName);
        const sourceConfig = getPluginSource(sourceName);
        if (!sourceConfig.structuredHeaders) return [];

        // Collect structured header fields that are only visible for SharedApiKey in the admin form.
        // These need to be collected from individual users in the UserApiKey flow.
        return sourceConfig.structuredHeaders.fields
            .filter((f) => f.visibleForAuthTypes && !f.visibleForAuthTypes.includes(AiPluginAuthType.UserApiKey))
            .map((f) => ({
                key: f.headerKey,
                label: f.label,
                placeholder: f.placeholder,
                helperText: f.helperText,
                required: f.required,
            }));
    }, [apiKeyModal]);

    const handleConnect = useCallback(
        async (pluginId: string, pluginName: string, authType: AiPluginAuthType, pluginUrl?: string | null) => {
            if (authType === AiPluginAuthType.UserOauth) {
                await initiateOAuthConnect(pluginId);
            } else {
                setApiKeyModal({ pluginId, pluginName, pluginUrl });
            }
        },
        [initiateOAuthConnect],
    );

    const handleApiKeySubmit = useCallback(
        async (apiKey: string, additionalHeaders?: { key: string; value: string }[]) => {
            if (!apiKeyModal) return;

            try {
                const response = await fetch(
                    `/integrations/oauth/plugins/${encodeURIComponent(apiKeyModal.pluginId)}/api-key`,
                    {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        credentials: 'include',
                        body: JSON.stringify({ api_key: apiKey }),
                    },
                );

                if (!response.ok) {
                    const errorData = await response.json().catch(() => ({}));
                    throw new Error(errorData.detail || `Failed to save API key: ${response.status}`);
                }

                // Save any additional headers as per-user custom headers
                if (additionalHeaders && additionalHeaders.length > 0) {
                    const existingHeaders =
                        availablePlugins.find((p) => p.id === apiKeyModal.pluginId)?.customHeaders || [];
                    const merged = mergeCustomHeaders(existingHeaders, additionalHeaders);

                    await updateUserSettings({
                        variables: {
                            input: {
                                pluginId: apiKeyModal.pluginId,
                                customHeaders: merged.map((h) => ({ key: h.key, value: h.value })),
                            },
                        },
                    });
                }

                message.success(`Connected to ${apiKeyModal.pluginName} successfully!`);
                refetch();
            } catch (err) {
                message.error(err instanceof Error ? err.message : 'Failed to save API key. Please try again.');
                throw err;
            }
        },
        [apiKeyModal, availablePlugins, updateUserSettings, refetch],
    );

    const handleToggleEnabled = useCallback(
        async (pluginId: string, enabled: boolean) => {
            setTogglingPluginId(pluginId);
            try {
                const result = await updateUserSettings({
                    variables: {
                        input: {
                            pluginId,
                            enabled,
                        },
                    },
                });
                if (result.errors && result.errors.length > 0) {
                    console.error('[ManageMyAiSettings] GraphQL errors:', result.errors);
                    message.error('Failed to update plugin. Please try again.');
                    return;
                }
                message.success(enabled ? 'Plugin enabled' : 'Plugin disabled');
                refetch();
            } catch (err) {
                console.error('[ManageMyAiSettings] Toggle error:', err);
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
                    headers: { 'Content-Type': 'application/json' },
                    credentials: 'include',
                });

                if (!response.ok) {
                    const errorData = await response.json().catch(() => ({}));
                    throw new Error(errorData.detail || `Failed to disconnect: ${response.status}`);
                }

                message.success(`Disconnected from ${pluginName}`);
                refetch();
            } catch (err) {
                message.error(err instanceof Error ? err.message : 'Failed to disconnect. Please try again.');
            }
        },
        [refetch],
    );

    const showLoading = loading && availablePlugins.length === 0;
    const showEmptyState = !showLoading && availablePlugins.length === 0;

    const renderContent = () => {
        if (showLoading) {
            return <Spin style={{ marginTop: 40 }} />;
        }

        if (showEmptyState) {
            return (
                <Empty
                    style={{ marginTop: 40 }}
                    description={
                        <span>
                            No AI plugins are available for use.
                            <br />
                            Contact your administrator to connect DataHub to external tools.
                        </span>
                    }
                />
            );
        }

        return (
            <PluginsList data-testid="plugins-list">
                {availablePlugins.map((plugin) => (
                    <IntegrationCard
                        key={plugin.id}
                        plugin={plugin}
                        isConnected={plugin.isConnected}
                        isEnabled={plugin.isEnabled}
                        onConnect={() =>
                            handleConnect(
                                plugin.id,
                                plugin.service?.properties?.displayName || 'Unknown Plugin',
                                plugin.authType,
                                plugin.service?.mcpServerProperties?.url,
                            )
                        }
                        onToggleEnabled={(enabled) => handleToggleEnabled(plugin.id, enabled)}
                        onDisconnect={() =>
                            handleDisconnect(plugin.id, plugin.service?.properties?.displayName || 'Unknown Plugin')
                        }
                        onCustomHeaders={() =>
                            setCustomHeadersModal({
                                pluginId: plugin.id,
                                pluginName: plugin.service?.properties?.displayName || 'Unknown Plugin',
                            })
                        }
                        isConnecting={connectingPluginId === plugin.id}
                        isToggling={togglingPluginId === plugin.id}
                        hasCustomHeaders={plugin.customHeaders.length > 0}
                    />
                ))}
            </PluginsList>
        );
    };

    return (
        <Container data-testid="my-ai-settings-page">
            <PageTitle
                title="My AI Settings"
                subTitle="Manage your AI plugin preferences for Ask DataHub. Enable plugins to use them in chat."
            />

            {renderContent()}

            {/* API Key Modal */}
            <ApiKeyModal
                open={apiKeyModal !== null}
                pluginName={apiKeyModal?.pluginName || ''}
                onClose={() => setApiKeyModal(null)}
                onSubmit={handleApiKeySubmit}
                additionalFields={apiKeyAdditionalFields}
            />

            {/* Custom Headers Modal */}
            <CustomHeadersModal
                open={customHeadersModal !== null}
                pluginId={customHeadersModal?.pluginId || ''}
                pluginName={customHeadersModal?.pluginName || ''}
                existingHeaders={
                    customHeadersModal
                        ? availablePlugins.find((p) => p.id === customHeadersModal.pluginId)?.customHeaders || []
                        : []
                }
                onClose={() => setCustomHeadersModal(null)}
                onSaved={refetch}
            />
        </Container>
    );
};

export default ManageMyAiSettings;
