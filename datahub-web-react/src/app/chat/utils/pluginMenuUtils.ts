import { AiPluginAuthType } from '@types';

export interface PluginWithUserStatus {
    id: string;
    authType: AiPluginAuthType;
    isConnected: boolean;
    isEnabled: boolean;
    service?: {
        properties?: {
            displayName?: string | null;
        } | null;
        mcpServerProperties?: {
            url?: string | null;
        } | null;
    } | null;
}

export interface PluginDisplayInfo {
    displayName: string;
    url: string | null;
    needsUserConnection: boolean;
    isConnected: boolean;
    showConnectButton: boolean;
    showToggle: boolean;
}

/**
 * Determines if a plugin requires user-level authentication/connection.
 * User OAuth and User API Key require individual user connections.
 */
export function requiresUserConnection(authType: AiPluginAuthType): boolean {
    return authType === AiPluginAuthType.UserOauth || authType === AiPluginAuthType.UserApiKey;
}

/**
 * Gets display information for a plugin including visibility rules for UI elements.
 */
export function getPluginDisplayInfo(plugin: PluginWithUserStatus): PluginDisplayInfo {
    const displayName = plugin.service?.properties?.displayName || 'Unknown Plugin';
    const url = plugin.service?.mcpServerProperties?.url || null;
    const needsUserConnection = requiresUserConnection(plugin.authType);
    const { isConnected } = plugin;
    const showConnectButton = needsUserConnection && !isConnected;
    const showToggle = needsUserConnection ? isConnected : true;

    return {
        displayName,
        url,
        needsUserConnection,
        isConnected,
        showConnectButton,
        showToggle,
    };
}

/**
 * Determines if the plugin menu animation should be shown.
 * Animation shows when user hasn't seen it before AND there are plugins available.
 */
export function shouldShowPluginAnimation(hasSeenMenu: boolean, pluginCount: number): boolean {
    return !hasSeenMenu && pluginCount > 0;
}

/**
 * Gets the appropriate empty state message based on user's admin status.
 */
export function getEmptyStateMessage(isAdmin: boolean): string {
    return isAdmin
        ? 'No AI plugins have been set up yet.'
        : 'No AI plugins have been set up. Please contact your administrator to set them up.';
}
