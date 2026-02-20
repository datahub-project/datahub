import { message } from 'antd';
import { useCallback } from 'react';

import analytics, { EventType } from '@app/analytics';
import { AiPluginRow } from '@app/settingsV2/platform/ai/plugins/utils/pluginDataUtils';
import { extractPlatformFromUrl } from '@app/settingsV2/platform/ai/plugins/utils/pluginLogoUtils';
import { useDeleteAiPluginMutation, useUpsertAiPluginMutation } from '@src/graphql/aiPlugins.generated';
import { ServiceSubType } from '@src/types.generated';

type UsePluginActionsOptions = {
    onSuccess?: () => void;
};

type UsePluginActionsResult = {
    handleToggleEnabled: (pluginRow: AiPluginRow) => Promise<void>;
    handleDelete: (pluginRow: AiPluginRow) => Promise<void>;
    isDeleting: boolean;
    isUpdating: boolean;
};

/**
 * Builds the mutation input for toggling a plugin's enabled state.
 */
export function buildToggleInput(pluginRow: AiPluginRow) {
    const { plugin } = pluginRow;
    const { service } = plugin;

    return {
        id: service?.urn?.split(':').pop(),
        displayName: service?.properties?.displayName || plugin.id,
        description: service?.properties?.description || undefined,
        subType: ServiceSubType.McpServer,
        mcpServerProperties: {
            url: service?.mcpServerProperties?.url || '',
            transport: service?.mcpServerProperties?.transport,
            timeout: service?.mcpServerProperties?.timeout || 30,
        },
        enabled: !plugin.enabled,
        authType: plugin.authType,
        instructions: plugin.instructions || undefined,
    };
}

/**
 * Hook for plugin actions (toggle enabled, delete).
 * Encapsulates mutation logic and provides callbacks for UI.
 */
export function usePluginActions({ onSuccess }: UsePluginActionsOptions = {}): UsePluginActionsResult {
    const [deleteAiPlugin, { loading: isDeleting }] = useDeleteAiPluginMutation();
    const [upsertAiPlugin, { loading: isUpdating }] = useUpsertAiPluginMutation();

    const handleToggleEnabled = useCallback(
        async (pluginRow: AiPluginRow) => {
            const { plugin } = pluginRow;
            const { service } = plugin;
            const urn = service?.urn;

            if (!urn) {
                message.error('Unable to update plugin');
                return;
            }

            try {
                await upsertAiPlugin({
                    variables: {
                        input: buildToggleInput(pluginRow),
                    },
                });
                message.success(`Plugin ${!plugin.enabled ? 'enabled' : 'disabled'}`);
                onSuccess?.();
            } catch (error) {
                message.error('Failed to update plugin');
                console.error('Error toggling plugin enabled:', error);
            }
        },
        [upsertAiPlugin, onSuccess],
    );

    const handleDelete = useCallback(
        async (pluginRow: AiPluginRow) => {
            try {
                await deleteAiPlugin({ variables: { urn: pluginRow.id } });

                // Emit analytics event
                const pluginUrl = pluginRow.plugin.service?.mcpServerProperties?.url;
                analytics.event({
                    type: EventType.DeleteAiPluginEvent,
                    pluginId: pluginRow.id,
                    pluginType: pluginRow.plugin.type,
                    authType: pluginRow.authType,
                    platform: extractPlatformFromUrl(pluginUrl),
                });

                message.success('Plugin deleted successfully');
                onSuccess?.();
            } catch (error) {
                message.error('Failed to delete plugin');
                console.error('Error deleting plugin:', error);
            }
        },
        [deleteAiPlugin, onSuccess],
    );

    return {
        handleToggleEnabled,
        handleDelete,
        isDeleting,
        isUpdating,
    };
}
