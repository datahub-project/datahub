import i18next from 'i18next';
import { useCallback } from 'react';

import type { McpServerRow } from '@app/settingsV2/platform/ai/mcpServers/utils/mcpServerDataUtils';
import { toast } from '@src/alchemy-components';

import { useUpdateMcpServersMutation } from '@graphql/mcpServers.generated';
import type { McpServerConfigFieldsFragment } from '@graphql/mcpServers.generated';
import type { McpServerConfigInput, UpdateMcpSettingsInput } from '@types';

type UseMcpServerActionsOptions = {
    onSuccess?: () => void;
};

/**
 * Map a stored config into the upsert input, keeping every field, with overrides applied.
 * The mutation replaces an included server, so a field left out here would be cleared.
 */
export function configToInput(
    config: McpServerConfigFieldsFragment | null,
    overrides: Partial<McpServerConfigInput>,
): McpServerConfigInput {
    return {
        slug: config?.slug ?? undefined,
        name: config?.name ?? '',
        description: config?.description ?? undefined,
        enabled: config?.enabled ?? true,
        customInstructions: config?.customInstructions ?? undefined,
        instructionSize: config?.instructionSize ?? undefined,
        tools: config?.tools?.map((t) => ({ refType: t.refType, toolId: t.toolId, enabled: t.enabled })),
        viewUrn: config?.viewUrn ?? undefined,
        ...overrides,
    };
}

/** The settings input that upserts one server: the default server's overrides, or a scoped server. */
export function serverUpdateInput(isDefault: boolean, server: McpServerConfigInput): UpdateMcpSettingsInput {
    return isDefault ? { defaultMcpServerOverrides: server } : { servers: [server] };
}

export function useMcpServerActions({ onSuccess }: UseMcpServerActionsOptions = {}) {
    const [updateMcpServers] = useUpdateMcpServersMutation();

    const handleToggleEnabled = useCallback(
        async (row: McpServerRow) => {
            const nextEnabled = !row.enabled;
            const serverInput = configToInput(row.config, {
                name: row.config?.name || row.name,
                enabled: nextEnabled,
            });
            try {
                await updateMcpServers({ variables: { input: serverUpdateInput(row.isDefault, serverInput) } });
                toast.success(
                    nextEnabled
                        ? i18next.t('settings.ai:platform.ai.mcpServers.enabled')
                        : i18next.t('settings.ai:platform.ai.mcpServers.disabled'),
                );
                onSuccess?.();
            } catch (e) {
                toast.error(i18next.t('settings.ai:platform.ai.mcpServers.updateFailed'));
                console.error('Error toggling MCP server enabled:', e);
            }
        },
        [updateMcpServers, onSuccess],
    );

    // Master on/off switch for all MCP servers (McpSettings.enabled).
    const handleToggleGlobalEnabled = useCallback(
        async (enabled: boolean) => {
            try {
                await updateMcpServers({ variables: { input: { enabled } } });
                onSuccess?.();
            } catch (e) {
                toast.error(i18next.t('settings.ai:platform.ai.mcpServers.updateFailed'));
                console.error('Error toggling MCP master switch:', e);
            }
        },
        [updateMcpServers, onSuccess],
    );

    return { handleToggleEnabled, handleToggleGlobalEnabled };
}
