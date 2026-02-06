import { describe, expect, it } from 'vitest';

import {
    AiPluginRow,
    createDuplicatePlugin,
    transformPluginsToRows,
} from '@app/settingsV2/platform/ai/plugins/utils/pluginDataUtils';
import { AiPluginAuthType, AiPluginConfig } from '@src/types.generated';

describe('pluginDataUtils', () => {
    describe('transformPluginsToRows', () => {
        it('returns empty array for null/undefined input', () => {
            expect(transformPluginsToRows(null)).toEqual([]);
            expect(transformPluginsToRows(undefined)).toEqual([]);
        });

        it('returns empty array for empty array input', () => {
            expect(transformPluginsToRows([])).toEqual([]);
        });

        it('transforms plugins to rows with correct fields', () => {
            const plugins = [
                {
                    id: 'plugin-1',
                    authType: AiPluginAuthType.None,
                    enabled: true,
                    service: {
                        urn: 'urn:li:service:plugin-1',
                        properties: {
                            displayName: 'Test Plugin',
                        },
                        mcpServerProperties: {
                            url: 'https://example.com/mcp',
                        },
                    },
                },
            ] as AiPluginConfig[];

            const rows = transformPluginsToRows(plugins);

            expect(rows).toHaveLength(1);
            expect(rows[0]).toMatchObject({
                id: 'plugin-1',
                name: 'Test Plugin',
                url: 'https://example.com/mcp',
                authType: AiPluginAuthType.None,
                enabled: true,
            });
        });

        it('uses plugin id as name when displayName is missing', () => {
            const plugins = [
                {
                    id: 'fallback-name',
                    authType: AiPluginAuthType.None,
                    enabled: false,
                },
            ] as AiPluginConfig[];

            const rows = transformPluginsToRows(plugins);

            expect(rows[0].name).toBe('fallback-name');
        });

        it('sorts rows alphabetically by name', () => {
            const plugins = [
                {
                    id: 'z-plugin',
                    authType: AiPluginAuthType.None,
                    enabled: true,
                    service: { properties: { displayName: 'Zebra' } },
                },
                {
                    id: 'a-plugin',
                    authType: AiPluginAuthType.None,
                    enabled: true,
                    service: { properties: { displayName: 'Alpha' } },
                },
            ] as AiPluginConfig[];

            const rows = transformPluginsToRows(plugins);

            expect(rows[0].name).toBe('Alpha');
            expect(rows[1].name).toBe('Zebra');
        });
    });

    describe('createDuplicatePlugin', () => {
        it('clears id and urn for new plugin creation', () => {
            const pluginRow = {
                id: 'original-id',
                name: 'Original Plugin',
                url: 'https://example.com',
                authType: AiPluginAuthType.SharedApiKey,
                enabled: true,
                plugin: {
                    id: 'original-id',
                    authType: AiPluginAuthType.SharedApiKey,
                    enabled: true,
                    service: {
                        urn: 'urn:li:service:original-id',
                        properties: {
                            displayName: 'Original Plugin',
                        },
                    },
                },
            } as AiPluginRow;

            const duplicate = createDuplicatePlugin(pluginRow);

            expect(duplicate.id).toBe('');
            expect(duplicate.service?.urn).toBe('');
        });

        it('appends (Copy) to display name', () => {
            const pluginRow = {
                id: 'original-id',
                name: 'My Plugin',
                url: null,
                authType: AiPluginAuthType.None,
                enabled: true,
                plugin: {
                    id: 'original-id',
                    authType: AiPluginAuthType.None,
                    enabled: true,
                    service: {
                        urn: 'urn:li:service:original-id',
                        properties: {
                            displayName: 'My Plugin',
                        },
                    },
                },
            } as AiPluginRow;

            const duplicate = createDuplicatePlugin(pluginRow);

            expect(duplicate.service?.properties?.displayName).toBe('My Plugin (Copy)');
        });

        it('handles plugin without service', () => {
            const pluginRow = {
                id: 'no-service',
                name: 'no-service',
                url: null,
                authType: AiPluginAuthType.None,
                enabled: false,
                plugin: {
                    id: 'no-service',
                    authType: AiPluginAuthType.None,
                    enabled: false,
                },
            } as AiPluginRow;

            const duplicate = createDuplicatePlugin(pluginRow);

            expect(duplicate.id).toBe('');
            expect(duplicate.service).toBeUndefined();
        });
    });
});
