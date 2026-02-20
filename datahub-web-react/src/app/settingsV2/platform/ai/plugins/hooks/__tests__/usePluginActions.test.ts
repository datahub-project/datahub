import { act, renderHook } from '@testing-library/react-hooks';
import { describe, expect, it, vi } from 'vitest';

import { buildToggleInput, usePluginActions } from '@app/settingsV2/platform/ai/plugins/hooks/usePluginActions';
import { AiPluginRow } from '@app/settingsV2/platform/ai/plugins/utils/pluginDataUtils';
import { AiPluginAuthType, AiPluginType, McpTransport, ServiceSubType } from '@src/types.generated';

// Mock the generated GraphQL hooks
const mockUpsertAiPlugin = vi.fn();
const mockDeleteAiPlugin = vi.fn();

vi.mock('@src/graphql/aiPlugins.generated', () => ({
    useUpsertAiPluginMutation: () => [mockUpsertAiPlugin, { loading: false }],
    useDeleteAiPluginMutation: () => [mockDeleteAiPlugin, { loading: false }],
}));

// Mock antd message
const mockMessage = vi.hoisted(() => ({
    success: vi.fn(),
    error: vi.fn(),
}));
vi.mock('antd', () => ({ message: mockMessage }));

// Mock analytics
const mockAnalyticsEvent = vi.hoisted(() => vi.fn());
vi.mock('@app/analytics', () => ({
    default: { event: mockAnalyticsEvent },
    EventType: { DeleteAiPluginEvent: 'DeleteAiPluginEvent' },
}));

function createPluginRow(overrides: Partial<AiPluginRow> = {}): AiPluginRow {
    return {
        id: 'urn:li:service:test-plugin',
        name: 'Test Plugin',
        url: 'https://example.com/mcp',
        authType: AiPluginAuthType.None,
        enabled: true,
        plugin: {
            id: 'urn:li:service:test-plugin',
            type: AiPluginType.McpServer,
            serviceUrn: 'urn:li:service:test-plugin',
            authType: AiPluginAuthType.None,
            enabled: true,
            instructions: null,
            service: {
                urn: 'urn:li:service:test-plugin',
                type: 'SERVICE' as any,
                properties: {
                    displayName: 'Test Plugin',
                    description: 'A test plugin',
                },
                mcpServerProperties: {
                    url: 'https://example.com/mcp',
                    timeout: 30,
                    transport: McpTransport.Http,
                    customHeaders: [],
                },
            },
        } as any,
        ...overrides,
    };
}

describe('usePluginActions', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        mockUpsertAiPlugin.mockResolvedValue({ data: { upsertAiPlugin: { urn: 'urn:li:service:test-plugin' } } });
        mockDeleteAiPlugin.mockResolvedValue({ data: { deleteAiPlugin: true } });
    });

    describe('buildToggleInput', () => {
        it('builds correct input for toggling enabled state', () => {
            const pluginRow = createPluginRow();
            const input = buildToggleInput(pluginRow);

            expect(input).toMatchObject({
                id: 'test-plugin',
                displayName: 'Test Plugin',
                description: 'A test plugin',
                subType: ServiceSubType.McpServer,
                mcpServerProperties: {
                    url: 'https://example.com/mcp',
                    timeout: 30,
                },
                enabled: false,
                authType: AiPluginAuthType.None,
            });
        });

        it('toggles enabled from false to true', () => {
            const pluginRow = createPluginRow({
                enabled: false,
                plugin: {
                    ...createPluginRow().plugin,
                    enabled: false,
                },
            });
            const input = buildToggleInput(pluginRow);
            expect(input.enabled).toBe(true);
        });

        it('uses default timeout when zero', () => {
            const row = createPluginRow();
            (row.plugin.service as any).mcpServerProperties.timeout = 0;
            const input = buildToggleInput(row);
            expect(input.mcpServerProperties.timeout).toBe(30);
        });

        it('uses plugin id as displayName fallback when properties missing', () => {
            const row = createPluginRow();
            (row.plugin.service as any).properties = {};
            const input = buildToggleInput(row);
            expect(input.displayName).toBe('urn:li:service:test-plugin');
        });
    });

    describe('handleToggleEnabled', () => {
        it('calls upsertAiPlugin and shows success message', async () => {
            const onSuccess = vi.fn();
            const { result } = renderHook(() => usePluginActions({ onSuccess }));

            await act(async () => {
                await result.current.handleToggleEnabled(createPluginRow());
            });

            expect(mockUpsertAiPlugin).toHaveBeenCalledOnce();
            expect(mockUpsertAiPlugin).toHaveBeenCalledWith({
                variables: { input: expect.objectContaining({ enabled: false }) },
            });
            expect(mockMessage.success).toHaveBeenCalledWith('Plugin disabled');
            expect(onSuccess).toHaveBeenCalledOnce();
        });

        it('shows "enabled" message when toggling from disabled', async () => {
            const row = createPluginRow({
                enabled: false,
                plugin: { ...createPluginRow().plugin, enabled: false },
            });
            const { result } = renderHook(() => usePluginActions());

            await act(async () => {
                await result.current.handleToggleEnabled(row);
            });

            expect(mockMessage.success).toHaveBeenCalledWith('Plugin enabled');
        });

        it('shows error when service URN is missing', async () => {
            const row = createPluginRow();
            (row.plugin.service as any).urn = undefined;
            const { result } = renderHook(() => usePluginActions());

            await act(async () => {
                await result.current.handleToggleEnabled(row);
            });

            expect(mockUpsertAiPlugin).not.toHaveBeenCalled();
            expect(mockMessage.error).toHaveBeenCalledWith('Unable to update plugin');
        });

        it('shows error on mutation failure', async () => {
            mockUpsertAiPlugin.mockRejectedValue(new Error('Network error'));
            const onSuccess = vi.fn();
            const { result } = renderHook(() => usePluginActions({ onSuccess }));

            await act(async () => {
                await result.current.handleToggleEnabled(createPluginRow());
            });

            expect(mockMessage.error).toHaveBeenCalledWith('Failed to update plugin');
            expect(onSuccess).not.toHaveBeenCalled();
        });
    });

    describe('handleDelete', () => {
        it('calls deleteAiPlugin with URN and shows success', async () => {
            const onSuccess = vi.fn();
            const { result } = renderHook(() => usePluginActions({ onSuccess }));

            await act(async () => {
                await result.current.handleDelete(createPluginRow());
            });

            expect(mockDeleteAiPlugin).toHaveBeenCalledWith({
                variables: { urn: 'urn:li:service:test-plugin' },
            });
            expect(mockMessage.success).toHaveBeenCalledWith('Plugin deleted successfully');
            expect(onSuccess).toHaveBeenCalledOnce();
        });

        it('emits analytics event on successful delete', async () => {
            const { result } = renderHook(() => usePluginActions());

            await act(async () => {
                await result.current.handleDelete(createPluginRow());
            });

            expect(mockAnalyticsEvent).toHaveBeenCalledWith(
                expect.objectContaining({
                    type: 'DeleteAiPluginEvent',
                    pluginId: 'urn:li:service:test-plugin',
                    authType: AiPluginAuthType.None,
                }),
            );
        });

        it('shows error on mutation failure', async () => {
            mockDeleteAiPlugin.mockRejectedValue(new Error('Forbidden'));
            const onSuccess = vi.fn();
            const { result } = renderHook(() => usePluginActions({ onSuccess }));

            await act(async () => {
                await result.current.handleDelete(createPluginRow());
            });

            expect(mockMessage.error).toHaveBeenCalledWith('Failed to delete plugin');
            expect(onSuccess).not.toHaveBeenCalled();
            expect(mockAnalyticsEvent).not.toHaveBeenCalled();
        });
    });
});
