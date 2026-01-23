import { describe, expect, it } from 'vitest';

import { getAuthTypeLabel } from '@app/settingsV2/platform/aiPlugins/utils/pluginDisplayUtils';
import { AiPluginAuthType } from '@src/types.generated';

describe('pluginDisplayUtils', () => {
    describe('getAuthTypeLabel', () => {
        it('returns OAuth for UserOauth type', () => {
            expect(getAuthTypeLabel(AiPluginAuthType.UserOauth)).toBe('OAuth');
        });

        it('returns Shared API Key for SharedApiKey type', () => {
            expect(getAuthTypeLabel(AiPluginAuthType.SharedApiKey)).toBe('Shared API Key');
        });

        it('returns User API Key for UserApiKey type', () => {
            expect(getAuthTypeLabel(AiPluginAuthType.UserApiKey)).toBe('User API Key');
        });

        it('returns None for None type', () => {
            expect(getAuthTypeLabel(AiPluginAuthType.None)).toBe('None');
        });
    });
});
