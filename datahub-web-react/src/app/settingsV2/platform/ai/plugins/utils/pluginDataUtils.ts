import { AiPluginAuthType, AiPluginConfig } from '@src/types.generated';

/**
 * Row representation of an AI plugin for table display.
 */
export type AiPluginRow = {
    id: string;
    name: string;
    url: string | null;
    authType: AiPluginAuthType;
    enabled: boolean;
    plugin: AiPluginConfig;
};

/**
 * Transforms API plugin data into table row format.
 * Sorts by name for consistent ordering.
 */
export function transformPluginsToRows(plugins: AiPluginConfig[] | undefined | null): AiPluginRow[] {
    if (!plugins) return [];

    return plugins
        .map((plugin) => ({
            id: plugin.id,
            name: plugin.service?.properties?.displayName || plugin.id,
            url: plugin.service?.mcpServerProperties?.url || null,
            authType: plugin.authType,
            enabled: plugin.enabled,
            plugin,
        }))
        .sort((a, b) => a.name.localeCompare(b.name));
}

/**
 * Creates a duplicate plugin configuration with cleared identifiers.
 * Used when duplicating an existing plugin to create a new one.
 */
export function createDuplicatePlugin(pluginRow: AiPluginRow): AiPluginConfig {
    const { plugin } = pluginRow;

    return {
        ...pluginRow,
        ...plugin,
        id: '', // Clear ID so it creates new
        service: plugin.service
            ? {
                  ...plugin.service,
                  urn: '', // Clear URN so it creates new
                  properties: {
                      ...plugin.service.properties,
                      displayName: `${plugin.service.properties?.displayName || plugin.id} (Copy)`,
                  },
              }
            : undefined,
    };
}
