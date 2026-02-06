import { describe, expect, it } from 'vitest';

import { DEFAULT_PLUGIN_FORM_STATE, PluginFormState } from '@app/settingsV2/platform/ai/plugins/utils/pluginFormState';
import {
    isFormValid,
    validateDisplayName,
    validateOAuthConfig,
    validatePluginForm,
    validateSharedApiKey,
    validateUrl,
    validateUrlFormat,
} from '@app/settingsV2/platform/ai/plugins/utils/pluginFormValidation';
import { AiPluginAuthType } from '@src/types.generated';

describe('pluginFormValidation', () => {
    describe('validateUrlFormat', () => {
        it('returns error for empty URL', () => {
            expect(validateUrlFormat('', 'Server URL')).toBe('Server URL is required');
            expect(validateUrlFormat('   ', 'Server URL')).toBe('Server URL is required');
        });

        it('returns null for valid https URL', () => {
            expect(validateUrlFormat('https://example.com', 'URL')).toBeNull();
            expect(validateUrlFormat('https://example.com/path', 'URL')).toBeNull();
            expect(validateUrlFormat('https://api.example.com/mcp/v1', 'URL')).toBeNull();
        });

        it('returns null for valid http URL', () => {
            expect(validateUrlFormat('http://localhost:8080', 'URL')).toBeNull();
            expect(validateUrlFormat('http://example.com/api', 'URL')).toBeNull();
        });

        it('returns error for URL without protocol', () => {
            expect(validateUrlFormat('example.com', 'Server URL')).toBe(
                'Server URL must be a valid URL (e.g., https://example.com)',
            );
            expect(validateUrlFormat('www.example.com/api', 'Server URL')).toBe(
                'Server URL must be a valid URL (e.g., https://example.com)',
            );
        });

        it('returns error for invalid protocol', () => {
            expect(validateUrlFormat('ftp://example.com', 'Server URL')).toBe(
                'Server URL must start with http:// or https://',
            );
            expect(validateUrlFormat('file:///path/to/file', 'Server URL')).toBe(
                'Server URL must start with http:// or https://',
            );
        });

        it('returns error for malformed URL', () => {
            expect(validateUrlFormat('not a url', 'Server URL')).toBe(
                'Server URL must be a valid URL (e.g., https://example.com)',
            );
            // 'https://' without hostname is caught as invalid by URL constructor
            expect(validateUrlFormat('https://', 'Server URL')).toBe(
                'Server URL must be a valid URL (e.g., https://example.com)',
            );
        });

        it('uses custom field label in error messages', () => {
            expect(validateUrlFormat('', 'Authorization URL')).toBe('Authorization URL is required');
            expect(validateUrlFormat('ftp://example.com', 'Token URL')).toBe(
                'Token URL must start with http:// or https://',
            );
        });
    });

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
            expect(validateUrl('http://localhost:8080/api')).toBeNull();
        });

        it('returns error for invalid URL format', () => {
            expect(validateUrl('not-a-url')).toBe('Server URL must be a valid URL (e.g., https://example.com)');
            expect(validateUrl('example.com')).toBe('Server URL must be a valid URL (e.g., https://example.com)');
        });

        it('returns error for non-http protocol', () => {
            expect(validateUrl('ftp://example.com')).toBe('Server URL must start with http:// or https://');
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
        it('returns errors for missing required OAuth fields', () => {
            const state: PluginFormState = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                authType: AiPluginAuthType.UserOauth,
            };

            const errors = validateOAuthConfig(state, false);

            expect(errors.oauthServerName).toBe('Provider name is required');
            expect(errors.oauthClientId).toBe('Client ID is required');
            // Client Secret is optional
            expect(errors.oauthClientSecret).toBeUndefined();
            expect(errors.oauthAuthorizationUrl).toBe('Authorization URL is required');
            expect(errors.oauthTokenUrl).toBe('Token URL is required');
        });

        it('returns empty errors when all required OAuth fields provided', () => {
            const state: PluginFormState = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                authType: AiPluginAuthType.UserOauth,
                oauthServerName: 'OAuth Provider',
                oauthClientId: 'client-id',
                oauthClientSecret: '', // Optional - can be empty
                oauthAuthorizationUrl: 'https://provider.com/authorize',
                oauthTokenUrl: 'https://provider.com/token',
            };

            const errors = validateOAuthConfig(state, false);

            expect(Object.keys(errors)).toHaveLength(0);
        });

        it('client secret is always optional', () => {
            const state: PluginFormState = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                authType: AiPluginAuthType.UserOauth,
                oauthServerName: 'OAuth Provider',
                oauthClientId: 'client-id',
                oauthClientSecret: '', // Empty - should always be allowed
                oauthAuthorizationUrl: 'https://provider.com/authorize',
                oauthTokenUrl: 'https://provider.com/token',
            };

            // Optional when creating
            const createErrors = validateOAuthConfig(state, false);
            expect(createErrors.oauthClientSecret).toBeUndefined();

            // Also optional when editing
            const editErrors = validateOAuthConfig(state, true);
            expect(editErrors.oauthClientSecret).toBeUndefined();
        });

        it('validates OAuth URL formats', () => {
            const state: PluginFormState = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                authType: AiPluginAuthType.UserOauth,
                oauthServerName: 'OAuth Provider',
                oauthClientId: 'client-id',
                oauthAuthorizationUrl: 'not-a-url',
                oauthTokenUrl: 'also-not-a-url',
            };

            const errors = validateOAuthConfig(state, false);

            expect(errors.oauthAuthorizationUrl).toBe(
                'Authorization URL must be a valid URL (e.g., https://example.com)',
            );
            expect(errors.oauthTokenUrl).toBe('Token URL must be a valid URL (e.g., https://example.com)');
        });

        it('rejects non-http protocols for OAuth URLs', () => {
            const state: PluginFormState = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                authType: AiPluginAuthType.UserOauth,
                oauthServerName: 'OAuth Provider',
                oauthClientId: 'client-id',
                oauthAuthorizationUrl: 'ftp://provider.com/authorize',
                oauthTokenUrl: 'file:///local/token',
            };

            const errors = validateOAuthConfig(state, false);

            expect(errors.oauthAuthorizationUrl).toBe('Authorization URL must start with http:// or https://');
            expect(errors.oauthTokenUrl).toBe('Token URL must start with http:// or https://');
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
