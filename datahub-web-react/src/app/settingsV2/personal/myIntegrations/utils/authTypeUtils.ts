import { AiPluginAuthType } from '@types';

/**
 * Returns a user-friendly label for an AI plugin auth type.
 */
export function getAuthTypeLabel(authType: AiPluginAuthType): string {
    switch (authType) {
        case AiPluginAuthType.UserOauth:
            return 'OAuth';
        case AiPluginAuthType.UserApiKey:
            return 'API Key';
        case AiPluginAuthType.SharedApiKey:
            return 'Shared Key';
        case AiPluginAuthType.None:
            return 'Public';
        default:
            return 'Unknown';
    }
}

/**
 * Returns true if the given auth type requires user connection (OAuth or API key).
 */
export function requiresUserConnection(authType: AiPluginAuthType): boolean {
    return authType === AiPluginAuthType.UserOauth || authType === AiPluginAuthType.UserApiKey;
}
