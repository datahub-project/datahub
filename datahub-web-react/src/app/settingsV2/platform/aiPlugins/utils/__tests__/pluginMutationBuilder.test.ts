import { describe, expect, it } from 'vitest';

import { DEFAULT_PLUGIN_FORM_STATE, PluginFormState } from '@app/settingsV2/platform/aiPlugins/utils/pluginFormState';
import {
    buildCustomHeadersInput,
    buildNewOAuthServerInput,
    buildUpsertOAuthServerInput,
    buildUpsertServiceInput,
    extractOAuthServerIdFromUrn,
    parseCommaSeparatedList,
} from '@app/settingsV2/platform/aiPlugins/utils/pluginMutationBuilder';
import { AiPluginAuthType, McpTransport, ServiceSubType } from '@src/types.generated';

describe('pluginMutationBuilder', () => {
    describe('parseCommaSeparatedList', () => {
        it('returns undefined for empty string', () => {
            expect(parseCommaSeparatedList('')).toBeUndefined();
            expect(parseCommaSeparatedList('   ')).toBeUndefined();
        });

        it('parses comma-separated values', () => {
            expect(parseCommaSeparatedList('a, b, c')).toEqual(['a', 'b', 'c']);
        });

        it('trims whitespace', () => {
            expect(parseCommaSeparatedList('  a  ,  b  ,  c  ')).toEqual(['a', 'b', 'c']);
        });

        it('filters empty values', () => {
            expect(parseCommaSeparatedList('a, , b, , c')).toEqual(['a', 'b', 'c']);
        });

        it('handles single value', () => {
            expect(parseCommaSeparatedList('single')).toEqual(['single']);
        });
    });

    describe('buildCustomHeadersInput', () => {
        it('returns undefined for empty headers', () => {
            const state = { ...DEFAULT_PLUGIN_FORM_STATE };
            expect(buildCustomHeadersInput(state)).toBeUndefined();
        });

        it('filters headers with empty keys', () => {
            const state: PluginFormState = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                customHeaders: [
                    { id: '1', key: '', value: 'value1' },
                    { id: '2', key: 'valid-key', value: 'value2' },
                ],
            };
            const result = buildCustomHeadersInput(state);

            expect(result).toHaveLength(1);
            expect(result![0]).toEqual({ key: 'valid-key', value: 'value2' });
        });

        it('builds headers correctly', () => {
            const state: PluginFormState = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                customHeaders: [
                    { id: '1', key: 'x-header-1', value: 'value1' },
                    { id: '2', key: 'x-header-2', value: 'value2' },
                ],
            };
            const result = buildCustomHeadersInput(state);

            expect(result).toEqual([
                { key: 'x-header-1', value: 'value1' },
                { key: 'x-header-2', value: 'value2' },
            ]);
        });
    });

    describe('buildNewOAuthServerInput', () => {
        it('returns undefined for non-OAuth auth type', () => {
            const state = { ...DEFAULT_PLUGIN_FORM_STATE };
            expect(buildNewOAuthServerInput(state)).toBeUndefined();
        });

        it('builds OAuth server input for UserOauth (new server)', () => {
            const state: PluginFormState = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                authType: AiPluginAuthType.UserOauth,
                oauthServerName: 'OAuth Provider',
                oauthServerDescription: 'Description',
                oauthClientId: 'client-id',
                oauthClientSecret: 'client-secret',
                oauthAuthorizationUrl: 'https://provider.com/authorize',
                oauthTokenUrl: 'https://provider.com/token',
                oauthScopes: 'openid, profile, email',
            };

            const result = buildNewOAuthServerInput(state);

            expect(result).toEqual({
                displayName: 'OAuth Provider',
                description: 'Description',
                clientId: 'client-id',
                clientSecret: 'client-secret',
                authorizationUrl: 'https://provider.com/authorize',
                tokenUrl: 'https://provider.com/token',
                scopes: ['openid', 'profile', 'email'],
                // Advanced OAuth settings (defaults)
                tokenAuthMethod: 'BASIC',
                authLocation: 'HEADER',
                authHeaderName: 'Authorization',
                authScheme: 'Bearer',
                authQueryParam: undefined,
            });
        });

        it('does not include id field (new servers only)', () => {
            const state: PluginFormState = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                authType: AiPluginAuthType.UserOauth,
                oauthServerName: 'OAuth Provider',
                oauthClientId: 'client-id',
                oauthClientSecret: 'client-secret',
                oauthAuthorizationUrl: 'https://provider.com/authorize',
                oauthTokenUrl: 'https://provider.com/token',
            };

            const result = buildNewOAuthServerInput(state);

            expect(result?.id).toBeUndefined();
        });
    });

    describe('buildUpsertOAuthServerInput', () => {
        it('builds OAuth server input for updating existing server', () => {
            const state: PluginFormState = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                authType: AiPluginAuthType.UserOauth,
                oauthServerName: 'Updated OAuth Provider',
                oauthServerDescription: 'Updated Description',
                oauthClientId: 'updated-client-id',
                oauthClientSecret: '', // Empty = preserve existing
                oauthAuthorizationUrl: 'https://provider.com/authorize',
                oauthTokenUrl: 'https://provider.com/token',
                oauthScopes: 'openid, profile',
            };

            const result = buildUpsertOAuthServerInput(state, 'existing-oauth-id');

            expect(result.id).toBe('existing-oauth-id');
            expect(result.displayName).toBe('Updated OAuth Provider');
            expect(result.clientId).toBe('updated-client-id');
            expect(result.clientSecret).toBeUndefined(); // undefined = preserve existing
        });

        it('includes client secret when provided', () => {
            const state: PluginFormState = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                authType: AiPluginAuthType.UserOauth,
                oauthServerName: 'OAuth Provider',
                oauthClientId: 'client-id',
                oauthClientSecret: 'new-secret', // New secret provided
                oauthAuthorizationUrl: 'https://provider.com/authorize',
                oauthTokenUrl: 'https://provider.com/token',
            };

            const result = buildUpsertOAuthServerInput(state, 'existing-oauth-id');

            expect(result.clientSecret).toBe('new-secret');
        });
    });

    describe('buildUpsertServiceInput', () => {
        it('builds basic input correctly', () => {
            const state: PluginFormState = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                displayName: 'My Plugin',
                description: 'Description',
                url: 'https://example.com/mcp',
                transport: McpTransport.Sse,
                timeout: '30',
                enabled: true,
            };

            const result = buildUpsertServiceInput(state, {});

            expect(result).toEqual({
                id: undefined,
                displayName: 'My Plugin',
                description: 'Description',
                subType: ServiceSubType.McpServer,
                mcpServerProperties: {
                    url: 'https://example.com/mcp',
                    transport: McpTransport.Sse,
                    timeout: 30,
                    customHeaders: undefined,
                },
                enabled: true,
                instructions: undefined,
                authType: AiPluginAuthType.None,
                newOAuthServer: undefined,
                sharedApiKey: undefined,
                sharedApiKeyAuthScheme: undefined,
                requiredScopes: undefined,
            });
        });

        it('extracts ID from editing URN', () => {
            const state = { ...DEFAULT_PLUGIN_FORM_STATE, displayName: 'My Plugin', url: 'https://example.com' };
            const result = buildUpsertServiceInput(state, {
                editingUrn: 'urn:li:service:my-plugin-id',
            });

            expect(result.id).toBe('my-plugin-id');
        });

        it('includes SharedApiKey fields when applicable', () => {
            const state: PluginFormState = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                displayName: 'My Plugin',
                url: 'https://example.com/mcp',
                authType: AiPluginAuthType.SharedApiKey,
                sharedApiKey: 'my-api-key',
                sharedApiKeyAuthScheme: 'Token',
            };

            const result = buildUpsertServiceInput(state, {});

            expect(result.sharedApiKey).toBe('my-api-key');
            expect(result.sharedApiKeyAuthScheme).toBe('Token');
        });

        it('does not include SharedApiKey fields for other auth types', () => {
            const state: PluginFormState = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                displayName: 'My Plugin',
                url: 'https://example.com/mcp',
                authType: AiPluginAuthType.None,
                sharedApiKey: 'should-be-ignored',
            };

            const result = buildUpsertServiceInput(state, {});

            expect(result.sharedApiKey).toBeUndefined();
        });

        it('parses required scopes', () => {
            const state: PluginFormState = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                displayName: 'My Plugin',
                url: 'https://example.com/mcp',
                requiredScopes: 'read, write, delete',
            };

            const result = buildUpsertServiceInput(state, {});

            expect(result.requiredScopes).toEqual(['read', 'write', 'delete']);
        });

        it('parses timeout as integer', () => {
            const state: PluginFormState = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                displayName: 'My Plugin',
                url: 'https://example.com/mcp',
                timeout: '45',
            };

            const result = buildUpsertServiceInput(state, {});

            expect(result.mcpServerProperties.timeout).toBe(45);
        });

        it('defaults timeout to 30 for invalid input', () => {
            const state: PluginFormState = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                displayName: 'My Plugin',
                url: 'https://example.com/mcp',
                timeout: 'invalid',
            };

            const result = buildUpsertServiceInput(state, {});

            expect(result.mcpServerProperties.timeout).toBe(30);
        });

        it('uses oauthServerUrn when editing with existing OAuth server', () => {
            const state: PluginFormState = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                displayName: 'My Plugin',
                url: 'https://example.com/mcp',
                authType: AiPluginAuthType.UserOauth,
                oauthServerName: 'OAuth Provider',
                oauthClientId: 'client-id',
                oauthAuthorizationUrl: 'https://provider.com/authorize',
                oauthTokenUrl: 'https://provider.com/token',
            };

            const result = buildUpsertServiceInput(state, {
                editingUrn: 'urn:li:service:my-plugin-id',
                existingOAuthServerUrn: 'urn:li:oauthAuthorizationServer:existing-oauth-id',
            });

            expect(result.oauthServerUrn).toBe('urn:li:oauthAuthorizationServer:existing-oauth-id');
            expect(result.newOAuthServer).toBeUndefined();
        });

        it('uses newOAuthServer when creating new plugin with OAuth', () => {
            const state: PluginFormState = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                displayName: 'My Plugin',
                url: 'https://example.com/mcp',
                authType: AiPluginAuthType.UserOauth,
                oauthServerName: 'New OAuth Provider',
                oauthClientId: 'client-id',
                oauthClientSecret: 'client-secret',
                oauthAuthorizationUrl: 'https://provider.com/authorize',
                oauthTokenUrl: 'https://provider.com/token',
            };

            const result = buildUpsertServiceInput(state, {});

            expect(result.oauthServerUrn).toBeUndefined();
            expect(result.newOAuthServer).toBeDefined();
            expect(result.newOAuthServer?.displayName).toBe('New OAuth Provider');
        });
    });

    describe('extractOAuthServerIdFromUrn', () => {
        it('returns null for null/undefined', () => {
            expect(extractOAuthServerIdFromUrn(null)).toBeNull();
            expect(extractOAuthServerIdFromUrn(undefined)).toBeNull();
        });

        it('extracts ID from URN', () => {
            expect(extractOAuthServerIdFromUrn('urn:li:oauthAuthorizationServer:my-oauth-id')).toBe('my-oauth-id');
        });

        it('returns null for empty string', () => {
            expect(extractOAuthServerIdFromUrn('')).toBeNull();
        });
    });
});
