import { describe, expect, it } from 'vitest';

import { getOverride, shouldShowField } from '@app/settingsV2/platform/ai/plugins/sources/pluginSourceUtils';
import { PluginSourceConfig } from '@app/settingsV2/platform/ai/plugins/sources/pluginSources.types';
import { AiPluginAuthType } from '@src/types.generated';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Minimal source config with a single auth type (OAuth-only, like GitHub) */
const singleAuthSource: PluginSourceConfig = {
    name: 'github',
    displayName: 'GitHub',
    description: 'Test',
    allowedAuthTypes: [AiPluginAuthType.UserOauth],
    defaults: {},
    visibleFields: ['displayName', 'url', 'oauthClientId', 'oauthScopes'],
    advancedFields: ['timeout'],
};

/** Source config with multiple auth types (like dbt or Custom) */
const multiAuthSource: PluginSourceConfig = {
    name: 'custom',
    displayName: 'Custom',
    description: 'Test',
    allowedAuthTypes: [
        AiPluginAuthType.None,
        AiPluginAuthType.SharedApiKey,
        AiPluginAuthType.UserApiKey,
        AiPluginAuthType.UserOauth,
    ],
    defaults: {},
    visibleFields: ['displayName', 'url', 'sharedApiKey', 'oauthClientId', 'oauthServerName', 'oauthAuthorizationUrl'],
    advancedFields: ['timeout', 'sharedApiKeyAuthScheme', 'userApiKeyAuthScheme', 'oauthTokenAuthMethod'],
    fieldOverrides: {
        url: { placeholder: 'https://custom.example.com', helperText: 'Custom helper' },
        sharedApiKey: { label: 'Custom API Key' },
    },
};

// ---------------------------------------------------------------------------
// shouldShowField
// ---------------------------------------------------------------------------

describe('shouldShowField', () => {
    describe('basic field list matching', () => {
        it('returns true when the field is in the list', () => {
            expect(
                shouldShowField(
                    'displayName',
                    singleAuthSource.visibleFields,
                    singleAuthSource,
                    AiPluginAuthType.UserOauth,
                ),
            ).toBe(true);
        });

        it('returns false when the field is NOT in the list', () => {
            expect(
                shouldShowField(
                    'description',
                    singleAuthSource.visibleFields,
                    singleAuthSource,
                    AiPluginAuthType.UserOauth,
                ),
            ).toBe(false);
        });

        it('works for advanced fields', () => {
            expect(
                shouldShowField(
                    'timeout',
                    singleAuthSource.advancedFields,
                    singleAuthSource,
                    AiPluginAuthType.UserOauth,
                ),
            ).toBe(true);
            expect(
                shouldShowField(
                    'displayName',
                    singleAuthSource.advancedFields,
                    singleAuthSource,
                    AiPluginAuthType.UserOauth,
                ),
            ).toBe(false);
        });
    });

    describe('single auth type source (no auth filtering)', () => {
        it('shows OAuth fields regardless of auth type argument for single-auth source', () => {
            // Single-auth sources don't do auth-type filtering — all fields in the list are shown
            expect(
                shouldShowField(
                    'oauthClientId',
                    singleAuthSource.visibleFields,
                    singleAuthSource,
                    AiPluginAuthType.UserOauth,
                ),
            ).toBe(true);
        });

        it('shows non-auth fields normally', () => {
            expect(
                shouldShowField('url', singleAuthSource.visibleFields, singleAuthSource, AiPluginAuthType.UserOauth),
            ).toBe(true);
        });
    });

    describe('multi auth type source (conditional auth filtering)', () => {
        it('shows SharedApiKey fields when auth type is SharedApiKey', () => {
            expect(
                shouldShowField(
                    'sharedApiKey',
                    multiAuthSource.visibleFields,
                    multiAuthSource,
                    AiPluginAuthType.SharedApiKey,
                ),
            ).toBe(true);
        });

        it('hides SharedApiKey fields when auth type is NOT SharedApiKey', () => {
            expect(
                shouldShowField(
                    'sharedApiKey',
                    multiAuthSource.visibleFields,
                    multiAuthSource,
                    AiPluginAuthType.UserOauth,
                ),
            ).toBe(false);
            expect(
                shouldShowField('sharedApiKey', multiAuthSource.visibleFields, multiAuthSource, AiPluginAuthType.None),
            ).toBe(false);
        });

        it('shows OAuth fields when auth type is UserOauth', () => {
            expect(
                shouldShowField(
                    'oauthClientId',
                    multiAuthSource.visibleFields,
                    multiAuthSource,
                    AiPluginAuthType.UserOauth,
                ),
            ).toBe(true);
            expect(
                shouldShowField(
                    'oauthServerName',
                    multiAuthSource.visibleFields,
                    multiAuthSource,
                    AiPluginAuthType.UserOauth,
                ),
            ).toBe(true);
        });

        it('hides OAuth fields when auth type is NOT UserOauth', () => {
            expect(
                shouldShowField(
                    'oauthClientId',
                    multiAuthSource.visibleFields,
                    multiAuthSource,
                    AiPluginAuthType.SharedApiKey,
                ),
            ).toBe(false);
            expect(
                shouldShowField(
                    'oauthServerName',
                    multiAuthSource.visibleFields,
                    multiAuthSource,
                    AiPluginAuthType.None,
                ),
            ).toBe(false);
        });

        it('shows UserApiKey advanced fields when auth type is UserApiKey', () => {
            expect(
                shouldShowField(
                    'userApiKeyAuthScheme',
                    multiAuthSource.advancedFields,
                    multiAuthSource,
                    AiPluginAuthType.UserApiKey,
                ),
            ).toBe(true);
        });

        it('hides UserApiKey fields when auth type is NOT UserApiKey', () => {
            expect(
                shouldShowField(
                    'userApiKeyAuthScheme',
                    multiAuthSource.advancedFields,
                    multiAuthSource,
                    AiPluginAuthType.SharedApiKey,
                ),
            ).toBe(false);
        });

        it('shows SharedApiKey advanced fields when auth type is SharedApiKey', () => {
            expect(
                shouldShowField(
                    'sharedApiKeyAuthScheme',
                    multiAuthSource.advancedFields,
                    multiAuthSource,
                    AiPluginAuthType.SharedApiKey,
                ),
            ).toBe(true);
        });

        it('shows OAuth advanced fields when auth type is UserOauth', () => {
            expect(
                shouldShowField(
                    'oauthTokenAuthMethod',
                    multiAuthSource.advancedFields,
                    multiAuthSource,
                    AiPluginAuthType.UserOauth,
                ),
            ).toBe(true);
        });

        it('shows non-auth fields regardless of selected auth type', () => {
            expect(
                shouldShowField('displayName', multiAuthSource.visibleFields, multiAuthSource, AiPluginAuthType.None),
            ).toBe(true);
            expect(
                shouldShowField(
                    'displayName',
                    multiAuthSource.visibleFields,
                    multiAuthSource,
                    AiPluginAuthType.SharedApiKey,
                ),
            ).toBe(true);
            expect(
                shouldShowField('url', multiAuthSource.visibleFields, multiAuthSource, AiPluginAuthType.UserOauth),
            ).toBe(true);
            expect(
                shouldShowField('timeout', multiAuthSource.advancedFields, multiAuthSource, AiPluginAuthType.None),
            ).toBe(true);
        });
    });
});

// ---------------------------------------------------------------------------
// getOverride
// ---------------------------------------------------------------------------

describe('getOverride', () => {
    it('returns the override when defined', () => {
        const override = getOverride(multiAuthSource, 'url');
        expect(override.placeholder).toBe('https://custom.example.com');
        expect(override.helperText).toBe('Custom helper');
    });

    it('returns partial override (only defined properties)', () => {
        const override = getOverride(multiAuthSource, 'sharedApiKey');
        expect(override.label).toBe('Custom API Key');
        expect(override.placeholder).toBeUndefined();
        expect(override.helperText).toBeUndefined();
    });

    it('returns empty object when no override is defined for the field', () => {
        const override = getOverride(multiAuthSource, 'displayName');
        expect(override).toEqual({});
    });

    it('returns empty object when source has no fieldOverrides at all', () => {
        const override = getOverride(singleAuthSource, 'url');
        expect(override).toEqual({});
    });
});
