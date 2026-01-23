import { describe, expect, it } from 'vitest';

import { buildToggleInput } from '@app/settingsV2/platform/aiPlugins/hooks/usePluginActions';
import { AiPluginRow } from '@app/settingsV2/platform/aiPlugins/utils/pluginDataUtils';
import { AiPluginAuthType, McpTransport, ServiceSubType } from '@src/types.generated';

describe('usePluginActions', () => {
    describe('buildToggleInput', () => {
        it('builds correct input for toggling enabled state', () => {
            const pluginRow = {
                id: 'plugin-1',
                name: 'Test Plugin',
                url: 'https://example.com/mcp',
                authType: AiPluginAuthType.SharedApiKey,
                enabled: true,
                plugin: {
                    id: 'plugin-1',
                    authType: AiPluginAuthType.SharedApiKey,
                    enabled: true,
                    instructions: 'Test instructions',
                    service: {
                        urn: 'urn:li:service:plugin-1',
                        properties: {
                            displayName: 'Test Plugin',
                            description: 'A test plugin',
                        },
                        mcpServerProperties: {
                            url: 'https://example.com/mcp',
                            timeout: 60,
                            transport: McpTransport.Http,
                        },
                    },
                },
            } as AiPluginRow;

            const input = buildToggleInput(pluginRow);

            expect(input).toMatchObject({
                id: 'plugin-1',
                displayName: 'Test Plugin',
                description: 'A test plugin',
                subType: ServiceSubType.McpServer,
                mcpServerProperties: {
                    url: 'https://example.com/mcp',
                    timeout: 60,
                },
                enabled: false, // toggled from true to false
                authType: AiPluginAuthType.SharedApiKey,
                instructions: 'Test instructions',
            });
        });

        it('toggles enabled from false to true', () => {
            const pluginRow = {
                id: 'plugin-2',
                name: 'Disabled Plugin',
                url: null,
                authType: AiPluginAuthType.None,
                enabled: false,
                plugin: {
                    id: 'plugin-2',
                    authType: AiPluginAuthType.None,
                    enabled: false,
                    service: {
                        urn: 'urn:li:service:plugin-2',
                        properties: {
                            displayName: 'Disabled Plugin',
                        },
                        mcpServerProperties: {
                            url: '',
                            timeout: 30,
                            transport: McpTransport.Http,
                        },
                    },
                },
            } as AiPluginRow;

            const input = buildToggleInput(pluginRow);

            expect(input.enabled).toBe(true);
        });

        it('uses default timeout when zero', () => {
            const pluginRow = {
                id: 'plugin-3',
                name: 'No Timeout',
                url: 'https://example.com',
                authType: AiPluginAuthType.None,
                enabled: true,
                plugin: {
                    id: 'plugin-3',
                    authType: AiPluginAuthType.None,
                    enabled: true,
                    service: {
                        urn: 'urn:li:service:plugin-3',
                        properties: {
                            displayName: 'No Timeout',
                        },
                        mcpServerProperties: {
                            url: 'https://example.com',
                            timeout: 0,
                            transport: McpTransport.Http,
                        },
                    },
                },
            } as AiPluginRow;

            const input = buildToggleInput(pluginRow);

            expect(input.mcpServerProperties.timeout).toBe(30);
        });

        it('uses plugin id as displayName fallback', () => {
            const pluginRow = {
                id: 'fallback-id',
                name: 'fallback-id',
                url: null,
                authType: AiPluginAuthType.None,
                enabled: true,
                plugin: {
                    id: 'fallback-id',
                    authType: AiPluginAuthType.None,
                    enabled: true,
                    service: {
                        urn: 'urn:li:service:fallback-id',
                        properties: {},
                        mcpServerProperties: {
                            url: '',
                            timeout: 30,
                            transport: McpTransport.Http,
                        },
                    },
                },
            } as AiPluginRow;

            const input = buildToggleInput(pluginRow);

            expect(input.displayName).toBe('fallback-id');
        });
    });
});
