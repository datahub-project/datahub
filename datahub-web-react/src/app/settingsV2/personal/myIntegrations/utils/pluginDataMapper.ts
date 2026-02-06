import { requiresUserConnection } from '@app/settingsV2/personal/myIntegrations/utils/authTypeUtils';

import { AiPluginAuthType, StringMapEntry } from '@types';

/**
 * Represents a global plugin from admin configuration.
 */
export interface GlobalPlugin {
    id: string;
    enabled: boolean;
    authType: AiPluginAuthType;
    service?: {
        properties?: {
            displayName?: string | null;
            description?: string | null;
        } | null;
        mcpServerProperties?: {
            url?: string | null;
        } | null;
    } | null;
}

/**
 * Represents a user's plugin configuration.
 */
export interface UserPluginConfig {
    id: string;
    enabled?: boolean | null;
    oauthConfig?: {
        isConnected?: boolean | null;
    } | null;
    apiKeyConfig?: {
        isConnected?: boolean | null;
    } | null;
    customHeaders?: StringMapEntry[] | null;
}

/**
 * Represents a plugin with merged global and user data for display.
 */
export interface MergedPlugin extends GlobalPlugin {
    userConfig: UserPluginConfig | undefined;
    isConnected: boolean;
    isEnabled: boolean;
    customHeaders: StringMapEntry[];
}

/**
 * Determines if a user is connected to a plugin based on auth type and config.
 */
export function isUserConnected(authType: AiPluginAuthType, userConfig: UserPluginConfig | undefined): boolean {
    const needsConnection = requiresUserConnection(authType);

    if (!needsConnection) {
        // SHARED_API_KEY/NONE don't require user connection
        return true;
    }

    if (authType === AiPluginAuthType.UserOauth) {
        return userConfig?.oauthConfig?.isConnected || false;
    }

    // USER_API_KEY
    return userConfig?.apiKeyConfig?.isConnected || false;
}

/**
 * Merges global plugins with user configurations for display.
 * Only returns plugins that are enabled by admin.
 */
export function mergePluginsWithUserConfig(
    globalPlugins: GlobalPlugin[],
    userPlugins: UserPluginConfig[],
): MergedPlugin[] {
    return globalPlugins
        .filter((plugin) => plugin.enabled)
        .map((plugin) => {
            const userConfig = userPlugins.find((up) => up.id === plugin.id);
            const isConnected = isUserConnected(plugin.authType, userConfig);

            // User must explicitly enable the plugin
            const isEnabled = userConfig?.enabled === true;

            // Get custom headers from user config
            const customHeaders = (userConfig?.customHeaders || []) as StringMapEntry[];

            return {
                ...plugin,
                userConfig,
                isConnected,
                isEnabled,
                customHeaders,
            };
        });
}
