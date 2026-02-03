import { describe, expect, it } from 'vitest';

import {
    DEFAULT_PLUGIN_FORM_STATE,
    addCustomHeader,
    createFormStateFromPlugin,
    hasAdvancedSettings,
    removeCustomHeader,
    updateCustomHeader,
    updateFormField,
} from '@app/settingsV2/platform/aiPlugins/utils/pluginFormState';
import { AiPluginAuthType, McpTransport } from '@src/types.generated';

describe('pluginFormState', () => {
    describe('DEFAULT_PLUGIN_FORM_STATE', () => {
        it('has correct default values', () => {
            expect(DEFAULT_PLUGIN_FORM_STATE.displayName).toBe('');
            expect(DEFAULT_PLUGIN_FORM_STATE.url).toBe('');
            expect(DEFAULT_PLUGIN_FORM_STATE.transport).toBe(McpTransport.Sse);
            expect(DEFAULT_PLUGIN_FORM_STATE.timeout).toBe('30');
            expect(DEFAULT_PLUGIN_FORM_STATE.authType).toBe(AiPluginAuthType.None);
            expect(DEFAULT_PLUGIN_FORM_STATE.enabled).toBe(true);
            expect(DEFAULT_PLUGIN_FORM_STATE.customHeaders).toEqual([]);
        });
    });

    describe('createFormStateFromPlugin', () => {
        it('returns default state when plugin is null', () => {
            const result = createFormStateFromPlugin(null);
            expect(result).toEqual(DEFAULT_PLUGIN_FORM_STATE);
        });

        it('populates state from plugin data', () => {
            const plugin = {
                id: 'test-plugin',
                authType: AiPluginAuthType.SharedApiKey,
                enabled: false,
                instructions: 'Test instructions',
                service: {
                    urn: 'urn:li:service:test',
                    properties: {
                        displayName: 'Test Plugin',
                        description: 'A test plugin',
                    },
                    mcpServerProperties: {
                        url: 'https://example.com/mcp',
                        transport: McpTransport.Websocket,
                        timeout: 60,
                    },
                },
                sharedApiKeyConfig: {
                    authScheme: 'Token',
                },
                oauthConfig: null,
            };

            const result = createFormStateFromPlugin(plugin as any);

            expect(result.displayName).toBe('Test Plugin');
            expect(result.description).toBe('A test plugin');
            expect(result.url).toBe('https://example.com/mcp');
            expect(result.transport).toBe(McpTransport.Websocket);
            expect(result.timeout).toBe('60');
            expect(result.authType).toBe(AiPluginAuthType.SharedApiKey);
            expect(result.enabled).toBe(false);
            expect(result.instructions).toBe('Test instructions');
            expect(result.sharedApiKeyAuthScheme).toBe('Token');
        });

        it('populates custom headers from plugin data', () => {
            const plugin = {
                id: 'test-plugin',
                authType: AiPluginAuthType.None,
                enabled: true,
                service: {
                    mcpServerProperties: {
                        url: 'https://example.com/mcp',
                        customHeaders: [
                            { key: 'x-custom-1', value: 'value1' },
                            { key: 'x-custom-2', value: 'value2' },
                        ],
                    },
                },
            };

            const result = createFormStateFromPlugin(plugin as any);

            expect(result.customHeaders).toHaveLength(2);
            expect(result.customHeaders[0].key).toBe('x-custom-1');
            expect(result.customHeaders[0].value).toBe('value1');
            expect(result.customHeaders[1].key).toBe('x-custom-2');
            expect(result.customHeaders[1].value).toBe('value2');
        });
    });

    describe('updateFormField', () => {
        it('updates a single field immutably', () => {
            const state = { ...DEFAULT_PLUGIN_FORM_STATE };
            const newState = updateFormField(state, 'displayName', 'New Name');

            expect(newState.displayName).toBe('New Name');
            expect(state.displayName).toBe(''); // Original unchanged
            expect(newState).not.toBe(state); // New object
        });

        it('preserves other fields', () => {
            const state = { ...DEFAULT_PLUGIN_FORM_STATE, description: 'Original description' };
            const newState = updateFormField(state, 'displayName', 'New Name');

            expect(newState.description).toBe('Original description');
        });
    });

    describe('addCustomHeader', () => {
        it('adds a new empty header', () => {
            const state = { ...DEFAULT_PLUGIN_FORM_STATE };
            const newState = addCustomHeader(state);

            expect(newState.customHeaders).toHaveLength(1);
            expect(newState.customHeaders[0].key).toBe('');
            expect(newState.customHeaders[0].value).toBe('');
            expect(newState.customHeaders[0].id).toContain('header-');
        });

        it('appends to existing headers', () => {
            const state = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                customHeaders: [{ id: 'existing', key: 'x-existing', value: 'existing-value' }],
            };
            const newState = addCustomHeader(state);

            expect(newState.customHeaders).toHaveLength(2);
            expect(newState.customHeaders[0].id).toBe('existing');
        });
    });

    describe('updateCustomHeader', () => {
        it('updates header key', () => {
            const state = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                customHeaders: [{ id: 'header-1', key: '', value: '' }],
            };
            const newState = updateCustomHeader(state, 'header-1', 'key', 'x-new-header');

            expect(newState.customHeaders[0].key).toBe('x-new-header');
            expect(newState.customHeaders[0].value).toBe('');
        });

        it('updates header value', () => {
            const state = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                customHeaders: [{ id: 'header-1', key: 'x-header', value: '' }],
            };
            const newState = updateCustomHeader(state, 'header-1', 'value', 'new-value');

            expect(newState.customHeaders[0].key).toBe('x-header');
            expect(newState.customHeaders[0].value).toBe('new-value');
        });

        it('only updates matching header', () => {
            const state = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                customHeaders: [
                    { id: 'header-1', key: 'key1', value: 'value1' },
                    { id: 'header-2', key: 'key2', value: 'value2' },
                ],
            };
            const newState = updateCustomHeader(state, 'header-2', 'key', 'updated-key');

            expect(newState.customHeaders[0].key).toBe('key1');
            expect(newState.customHeaders[1].key).toBe('updated-key');
        });
    });

    describe('removeCustomHeader', () => {
        it('removes header by id', () => {
            const state = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                customHeaders: [
                    { id: 'header-1', key: 'key1', value: 'value1' },
                    { id: 'header-2', key: 'key2', value: 'value2' },
                ],
            };
            const newState = removeCustomHeader(state, 'header-1');

            expect(newState.customHeaders).toHaveLength(1);
            expect(newState.customHeaders[0].id).toBe('header-2');
        });
    });

    describe('hasAdvancedSettings', () => {
        it('returns false when no custom headers', () => {
            const state = { ...DEFAULT_PLUGIN_FORM_STATE };
            expect(hasAdvancedSettings(state)).toBe(false);
        });

        it('returns true when custom headers exist', () => {
            const state = {
                ...DEFAULT_PLUGIN_FORM_STATE,
                customHeaders: [{ id: 'header-1', key: 'x-header', value: 'value' }],
            };
            expect(hasAdvancedSettings(state)).toBe(true);
        });
    });
});
