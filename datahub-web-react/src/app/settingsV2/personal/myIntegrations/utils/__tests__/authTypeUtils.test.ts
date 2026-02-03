import { describe, expect, it } from 'vitest';

import { getAuthTypeLabel, requiresUserConnection } from '@app/settingsV2/personal/myIntegrations/utils/authTypeUtils';

import { AiPluginAuthType } from '@types';

describe('authTypeUtils', () => {
    describe('getAuthTypeLabel', () => {
        it('returns "OAuth" for UserOauth', () => {
            expect(getAuthTypeLabel(AiPluginAuthType.UserOauth)).toBe('OAuth');
        });

        it('returns "API Key" for UserApiKey', () => {
            expect(getAuthTypeLabel(AiPluginAuthType.UserApiKey)).toBe('API Key');
        });

        it('returns "Shared Key" for SharedApiKey', () => {
            expect(getAuthTypeLabel(AiPluginAuthType.SharedApiKey)).toBe('Shared Key');
        });

        it('returns "Public" for None', () => {
            expect(getAuthTypeLabel(AiPluginAuthType.None)).toBe('Public');
        });

        it('returns "Unknown" for unrecognized auth type', () => {
            expect(getAuthTypeLabel('SOMETHING_ELSE' as AiPluginAuthType)).toBe('Unknown');
        });
    });

    describe('requiresUserConnection', () => {
        it('returns true for UserOauth', () => {
            expect(requiresUserConnection(AiPluginAuthType.UserOauth)).toBe(true);
        });

        it('returns true for UserApiKey', () => {
            expect(requiresUserConnection(AiPluginAuthType.UserApiKey)).toBe(true);
        });

        it('returns false for SharedApiKey', () => {
            expect(requiresUserConnection(AiPluginAuthType.SharedApiKey)).toBe(false);
        });

        it('returns false for None', () => {
            expect(requiresUserConnection(AiPluginAuthType.None)).toBe(false);
        });
    });
});
