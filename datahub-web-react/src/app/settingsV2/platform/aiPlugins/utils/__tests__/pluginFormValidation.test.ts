import { describe, expect, it } from 'vitest';

import { DEFAULT_PLUGIN_FORM_STATE, PluginFormState } from '@app/settingsV2/platform/aiPlugins/utils/pluginFormState';
import {
    isFormValid,
    validateDisplayName,
    validateOAuthConfig,
    validatePluginForm,
    validateSharedApiKey,
    validateUrl,
} from '@app/settingsV2/platform/aiPlugins/utils/pluginFormValidation';
import { AiPluginAuthType } from '@src/types.generated';

describe('pluginFormValidation', () => {
    describe('validateDisplayName', () => {
        it('returns error for empty name', () => {
            expect(validateDisplayName('', [], null)).toBe('Name is required');
            expect(validateDisplayName('   ', [], null)).toBe('Name is required');
        });

        it('returns null for valid name', () => {
            expect(validateDisplayName('My Plugin', [], null)).toBeNull();
        });

        it('returns error for duplicate name', () => {
            const existingNames = ['Plugin A', 'Plugin B'];
            expect(validateDisplayName('Plugin A', existingNames, null)).toBe('A plugin with this name already exists');
        });

        it('is case insensitive for duplicate check', () => {
            const existingNames = ['Plugin A'];
            expect(validateDisplayName('plugin a', existingNames, null)).toBe('A plugin with this name already exists');
        });

        it('allows same name when editing original', () => {
            const existingNames = ['Plugin A', 'Plugin B'];
            expect(validateDisplayName('Plugin A', existingNames, 'Plugin A')).toBeNull();
        });
    });

    describe('validateUrl', () => {
        it('returns error for empty URL', () => {
            expect(validateUrl('')).toBe('Server URL is required');
            expect(validateUrl('   ')).toBe('Server URL is required');
        });

        it('returns null for valid URL', () => {
            expect(validateUrl('https://example.com/mcp')).toBeNull();
        });
    });

    describe('validateSharedApiKey', () => {
        it('returns error for empty key when not editing', () => {
            expect(validateSharedApiKey('', false)).toBe('Shared API key is required');
        });

        it('returns null for empty key when editing (optional)', () => {
            expect(validateSharedApiKey('', true)).toBeNull();
        });

        it('returns null for provided key', () => {
            expect(validateSharedApiKey('my-api-key', false)).toBeNull();
        });
    });

    describe('validateOAuthConfig', () => {
        it('returns errors for missing OAuth fields', () => {
            const state: PluginFormState = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                authType: AiPluginAuthType.UserOauth,
            };

            const errors = validateOAuthConfig(state);

            expect(errors.oauthServerName).toBe('Provider name is required');
            expect(errors.oauthClientId).toBe('Client ID is required');
            expect(errors.oauthClientSecret).toBe('Client Secret is required');
            expect(errors.oauthAuthorizationUrl).toBe('Authorization URL is required');
            expect(errors.oauthTokenUrl).toBe('Token URL is required');
        });

        it('returns empty errors when all OAuth fields provided', () => {
            const state: PluginFormState = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                authType: AiPluginAuthType.UserOauth,
                oauthServerName: 'OAuth Provider',
                oauthClientId: 'client-id',
                oauthClientSecret: 'client-secret',
                oauthAuthorizationUrl: 'https://provider.com/authorize',
                oauthTokenUrl: 'https://provider.com/token',
            };

            const errors = validateOAuthConfig(state);

            expect(Object.keys(errors)).toHaveLength(0);
        });
    });

    describe('validatePluginForm', () => {
        const defaultOptions = {
            isEditing: false,
            existingNames: [],
            originalName: null,
        };

        it('returns invalid for empty form', () => {
            const result = validatePluginForm(DEFAULT_PLUGIN_FORM_STATE, defaultOptions);

            expect(result.isValid).toBe(false);
            expect(result.errors.displayName).toBe('Name is required');
            expect(result.errors.url).toBe('Server URL is required');
        });

        it('returns valid for minimal form', () => {
            const state: PluginFormState = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                displayName: 'My Plugin',
                url: 'https://example.com/mcp',
            };

            const result = validatePluginForm(state, defaultOptions);

            expect(result.isValid).toBe(true);
            expect(Object.keys(result.errors)).toHaveLength(0);
        });

        it('validates SharedApiKey auth type', () => {
            const state: PluginFormState = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                displayName: 'My Plugin',
                url: 'https://example.com/mcp',
                authType: AiPluginAuthType.SharedApiKey,
            };

            const result = validatePluginForm(state, defaultOptions);

            expect(result.isValid).toBe(false);
            expect(result.errors.sharedApiKey).toBe('Shared API key is required');
        });

        it('validates UserOauth auth type', () => {
            const state: PluginFormState = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                displayName: 'My Plugin',
                url: 'https://example.com/mcp',
                authType: AiPluginAuthType.UserOauth,
            };

            const result = validatePluginForm(state, defaultOptions);

            expect(result.isValid).toBe(false);
            expect(result.errors.oauthServerName).toBeDefined();
            expect(result.errors.oauthClientId).toBeDefined();
        });
    });

    describe('isFormValid', () => {
        const defaultOptions = {
            isEditing: false,
            existingNames: [],
            originalName: null,
        };

        it('returns false for empty form', () => {
            expect(isFormValid(DEFAULT_PLUGIN_FORM_STATE, defaultOptions)).toBe(false);
        });

        it('returns true for valid minimal form', () => {
            const state: PluginFormState = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                displayName: 'My Plugin',
                url: 'https://example.com/mcp',
            };

            expect(isFormValid(state, defaultOptions)).toBe(true);
        });

        it('returns false for duplicate name', () => {
            const state: PluginFormState = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                displayName: 'Existing Plugin',
                url: 'https://example.com/mcp',
            };

            expect(isFormValid(state, { ...defaultOptions, existingNames: ['Existing Plugin'] })).toBe(false);
        });

        it('returns false for SharedApiKey without key', () => {
            const state: PluginFormState = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                displayName: 'My Plugin',
                url: 'https://example.com/mcp',
                authType: AiPluginAuthType.SharedApiKey,
            };

            expect(isFormValid(state, defaultOptions)).toBe(false);
        });

        it('returns true for SharedApiKey with key', () => {
            const state: PluginFormState = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                displayName: 'My Plugin',
                url: 'https://example.com/mcp',
                authType: AiPluginAuthType.SharedApiKey,
                sharedApiKey: 'my-api-key',
            };

            expect(isFormValid(state, defaultOptions)).toBe(true);
        });
    });
});
