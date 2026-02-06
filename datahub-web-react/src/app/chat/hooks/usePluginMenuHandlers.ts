import { message } from 'antd';
import { useCallback, useState } from 'react';
import { useHistory } from 'react-router-dom';

import { useOAuthConnect } from '@app/settingsV2/personal/aiConnections/useOAuthConnect';
import useUpdateEducationStep from '@providers/hooks/useUpdateEducationStep';

import { useUpdateUserAiPluginSettingsMutation } from '@graphql/aiPlugins.generated';
import { AiPluginAuthType } from '@types';

interface ApiKeyModalState {
    pluginId: string;
    pluginName: string;
}

interface UsePluginMenuHandlersProps {
    refetch: () => void;
    hasSeenPluginsMenu: boolean;
    availablePluginsCount: number;
    educationStepId: string;
}

export interface UsePluginMenuHandlersReturn {
    isOpen: boolean;
    setIsOpen: (open: boolean) => void;
    togglingPluginId: string | null;
    apiKeyModal: ApiKeyModalState | null;
    setApiKeyModal: (state: ApiKeyModalState | null) => void;
    connectingPluginId: string | null;
    handleConnect: (pluginId: string, pluginName: string, authType: AiPluginAuthType) => Promise<void>;
    handleApiKeySubmit: (apiKey: string) => Promise<void>;
    handleToggleEnabled: (pluginId: string, enabled: boolean) => Promise<void>;
    handleManageIntegrations: () => void;
    handleOpenChange: (open: boolean) => void;
    handleCreatePlugin: () => void;
}

/**
 * Custom hook for managing plugin menu handlers and state.
 * Encapsulates all event handlers and local state for the ChatPluginsMenu component.
 */
export const usePluginMenuHandlers = ({
    refetch,
    hasSeenPluginsMenu,
    availablePluginsCount,
    educationStepId,
}: UsePluginMenuHandlersProps): UsePluginMenuHandlersReturn => {
    const history = useHistory();
    const [updateUserSettings] = useUpdateUserAiPluginSettingsMutation();
    const { initiateOAuthConnect, connectingPluginId } = useOAuthConnect(refetch);
    const { updateEducationStep } = useUpdateEducationStep();
    const [togglingPluginId, setTogglingPluginId] = useState<string | null>(null);
    const [isOpen, setIsOpen] = useState(false);
    const [apiKeyModal, setApiKeyModal] = useState<ApiKeyModalState | null>(null);

    const handleConnect = useCallback(
        async (pluginId: string, pluginName: string, authType: AiPluginAuthType) => {
            if (authType === AiPluginAuthType.UserOauth) {
                await initiateOAuthConnect(pluginId);
            } else {
                setApiKeyModal({ pluginId, pluginName });
            }
        },
        [initiateOAuthConnect],
    );

    const handleApiKeySubmit = useCallback(
        async (apiKey: string) => {
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

                message.success(`Connected to ${apiKeyModal.pluginName} successfully!`);
                refetch();
            } catch (err) {
                message.error(err instanceof Error ? err.message : 'Failed to save API key. Please try again.');
                throw err;
            }
        },
        [apiKeyModal, refetch],
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
                    console.error('[ChatPluginsMenu] GraphQL errors:', result.errors);
                    return;
                }
                refetch();
            } catch (err) {
                console.error('[ChatPluginsMenu] Toggle error:', err);
            } finally {
                setTogglingPluginId(null);
            }
        },
        [updateUserSettings, refetch],
    );

    const handleManageIntegrations = useCallback(() => {
        setIsOpen(false);
        history.push('/settings/my-ai-settings');
    }, [history]);

    const handleCreatePlugin = useCallback(() => {
        setIsOpen(false);
        history.push('/settings/ai/plugins?create=true');
    }, [history]);

    const handleOpenChange = useCallback(
        (open: boolean) => {
            setIsOpen(open);
            // Mark as seen when user opens the menu for the first time
            if (open && !hasSeenPluginsMenu && availablePluginsCount > 0) {
                updateEducationStep(educationStepId);
            }
        },
        [hasSeenPluginsMenu, availablePluginsCount, updateEducationStep, educationStepId],
    );

    return {
        isOpen,
        setIsOpen,
        togglingPluginId,
        apiKeyModal,
        setApiKeyModal,
        connectingPluginId,
        handleConnect,
        handleApiKeySubmit,
        handleToggleEnabled,
        handleManageIntegrations,
        handleOpenChange,
        handleCreatePlugin,
    };
};
