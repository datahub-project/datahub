import { AiPluginAuthType } from '@src/types.generated';

/**
 * Maps the authType enum to a human-readable label.
 */
export function getAuthTypeLabel(authType: AiPluginAuthType): string {
    switch (authType) {
        case AiPluginAuthType.UserOauth:
            return 'OAuth';
        case AiPluginAuthType.SharedApiKey:
            return 'Shared API Key';
        case AiPluginAuthType.UserApiKey:
            return 'User API Key';
        case AiPluginAuthType.None:
        default:
            return 'None';
    }
}
