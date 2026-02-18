import { act, renderHook } from '@testing-library/react-hooks';
import { vi } from 'vitest';

import { useTestOAuthConnection } from '@app/settingsV2/platform/ai/plugins/hooks/useTestOAuthConnection';
import { DEFAULT_PLUGIN_FORM_STATE } from '@app/settingsV2/platform/ai/plugins/utils/pluginFormState';

const mockNotification = vi.hoisted(() => ({
    success: vi.fn(),
    error: vi.fn(),
    info: vi.fn(),
    warning: vi.fn(),
}));

vi.mock('@src/alchemy-components', () => ({
    notification: mockNotification,
}));

function createFormState(overrides: Record<string, unknown> = {}) {
    return {
        ...DEFAULT_PLUGIN_FORM_STATE,
        url: 'https://mcp.example.com',
        oauthClientId: 'test-client',
        oauthClientSecret: 'test-secret',
        oauthAuthorizationUrl: 'https://provider.com/authorize',
        oauthTokenUrl: 'https://provider.com/token',
        oauthScopes: 'read, write',
        ...overrides,
    };
}

describe('useTestOAuthConnection', () => {
    let messageListeners: ((event: MessageEvent) => void)[] = [];
    let mockPopup: { closed: boolean; close: ReturnType<typeof vi.fn>; location: { href: string } };

    beforeEach(() => {
        vi.clearAllMocks();
        vi.useFakeTimers();
        messageListeners = [];

        vi.spyOn(window, 'addEventListener').mockImplementation((type, listener) => {
            if (type === 'message') {
                messageListeners.push(listener as (event: MessageEvent) => void);
            }
        });

        vi.spyOn(window, 'removeEventListener').mockImplementation((type, listener) => {
            if (type === 'message') {
                messageListeners = messageListeners.filter((l) => l !== listener);
            }
        });

        mockPopup = { closed: false, close: vi.fn(), location: { href: '' } };
        vi.spyOn(window, 'open').mockReturnValue(mockPopup as unknown as Window);

        vi.spyOn(global, 'fetch').mockResolvedValue({
            ok: true,
            json: () => Promise.resolve({ authorization_url: 'https://provider.com/authorize?state=nonce123' }),
        } as Response);
    });

    afterEach(() => {
        vi.useRealTimers();
        vi.restoreAllMocks();
    });

    describe('Initialization', () => {
        it('should initialize with default state', () => {
            const { result } = renderHook(() => useTestOAuthConnection());

            expect(result.current.isTestingConnection).toBe(false);
            expect(result.current.testResult).toBeNull();
            expect(typeof result.current.testConnection).toBe('function');
            expect(typeof result.current.clearTestResult).toBe('function');
        });

        it('should register message event listener on mount', () => {
            renderHook(() => useTestOAuthConnection());
            expect(window.addEventListener).toHaveBeenCalledWith('message', expect.any(Function));
        });

        it('should remove message event listener on unmount', () => {
            const { unmount } = renderHook(() => useTestOAuthConnection());
            unmount();
            expect(window.removeEventListener).toHaveBeenCalledWith('message', expect.any(Function));
        });
    });

    describe('testConnection', () => {
        it('should open popup synchronously with about:blank', async () => {
            const { result } = renderHook(() => useTestOAuthConnection());

            await act(async () => {
                await result.current.testConnection(createFormState());
            });

            expect(window.open).toHaveBeenCalledWith(
                'about:blank',
                'oauth_test_connect',
                expect.stringContaining('width=600,height=700'),
            );
        });

        it('should navigate popup to authorization URL after fetch', async () => {
            const { result } = renderHook(() => useTestOAuthConnection());

            await act(async () => {
                await result.current.testConnection(createFormState());
            });

            expect(mockPopup.location.href).toBe('https://provider.com/authorize?state=nonce123');
        });

        it('should set isTestingConnection to true during flow', async () => {
            const { result } = renderHook(() => useTestOAuthConnection());

            await act(async () => {
                await result.current.testConnection(createFormState());
            });

            expect(result.current.isTestingConnection).toBe(true);
        });

        it('should call fetch with correct endpoint and body', async () => {
            const { result } = renderHook(() => useTestOAuthConnection());

            await act(async () => {
                await result.current.testConnection(createFormState());
            });

            expect(fetch).toHaveBeenCalledWith('/integrations/oauth/plugins/test-oauth-connect', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: expect.any(String),
            });

            const body = JSON.parse((fetch as ReturnType<typeof vi.fn>).mock.calls[0][1].body);
            expect(body.oauth_config.clientId).toBe('test-client');
            expect(body.oauth_config.authorizationUrl).toBe('https://provider.com/authorize');
            expect(body.oauth_config.scopes).toEqual(['read', 'write']);
            expect(body.mcp_config.url).toBe('https://mcp.example.com');
        });

        it('should parse scopes from comma-separated string', async () => {
            const { result } = renderHook(() => useTestOAuthConnection());

            await act(async () => {
                await result.current.testConnection(createFormState({ oauthScopes: 'read, write, admin' }));
            });

            const body = JSON.parse((fetch as ReturnType<typeof vi.fn>).mock.calls[0][1].body);
            expect(body.oauth_config.scopes).toEqual(['read', 'write', 'admin']);
        });

        it('should handle empty scopes', async () => {
            const { result } = renderHook(() => useTestOAuthConnection());

            await act(async () => {
                await result.current.testConnection(createFormState({ oauthScopes: '' }));
            });

            const body = JSON.parse((fetch as ReturnType<typeof vi.fn>).mock.calls[0][1].body);
            expect(body.oauth_config.scopes).toEqual([]);
        });

        it('should include custom headers in MCP config', async () => {
            const { result } = renderHook(() => useTestOAuthConnection());

            await act(async () => {
                await result.current.testConnection(
                    createFormState({
                        customHeaders: [
                            { id: '1', key: 'X-Custom', value: 'val1' },
                            { id: '2', key: 'X-Other', value: 'val2' },
                            { id: '3', key: '', value: 'ignored' }, // Empty key should be filtered
                        ],
                    }),
                );
            });

            const body = JSON.parse((fetch as ReturnType<typeof vi.fn>).mock.calls[0][1].body);
            expect(body.mcp_config.customHeaders).toEqual({ 'X-Custom': 'val1', 'X-Other': 'val2' });
        });

        it('should default transport to HTTP', async () => {
            const { result } = renderHook(() => useTestOAuthConnection());

            await act(async () => {
                await result.current.testConnection(createFormState({ transport: '' }));
            });

            const body = JSON.parse((fetch as ReturnType<typeof vi.fn>).mock.calls[0][1].body);
            expect(body.mcp_config.transport).toBe('HTTP');
        });

        it('should parse timeout as float', async () => {
            const { result } = renderHook(() => useTestOAuthConnection());

            await act(async () => {
                await result.current.testConnection(createFormState({ timeout: '45' }));
            });

            const body = JSON.parse((fetch as ReturnType<typeof vi.fn>).mock.calls[0][1].body);
            expect(body.mcp_config.timeout).toBe(45);
        });

        it('should default timeout to 30 when empty', async () => {
            const { result } = renderHook(() => useTestOAuthConnection());

            await act(async () => {
                await result.current.testConnection(createFormState({ timeout: '' }));
            });

            const body = JSON.parse((fetch as ReturnType<typeof vi.fn>).mock.calls[0][1].body);
            expect(body.mcp_config.timeout).toBe(30);
        });
    });

    describe('Popup Blocked', () => {
        it('should show notification when popup is blocked', async () => {
            vi.spyOn(window, 'open').mockReturnValue(null);

            const { result } = renderHook(() => useTestOAuthConnection());

            await act(async () => {
                await result.current.testConnection(createFormState());
            });

            expect(result.current.isTestingConnection).toBe(false);
            expect(mockNotification.error).toHaveBeenCalledWith({
                message: 'Popup Blocked',
                description: 'Please allow popups for this site and try again.',
            });
            expect(fetch).not.toHaveBeenCalled();
        });
    });

    describe('Error Handling', () => {
        it('should handle fetch error with detail', async () => {
            vi.spyOn(global, 'fetch').mockResolvedValue({
                ok: false,
                status: 400,
                json: () => Promise.resolve({ detail: 'Invalid config' }),
            } as Response);

            const { result } = renderHook(() => useTestOAuthConnection());

            await act(async () => {
                await result.current.testConnection(createFormState());
            });

            expect(result.current.isTestingConnection).toBe(false);
            expect(mockNotification.error).toHaveBeenCalledWith({
                message: 'Test Connection Failed',
                description: 'Invalid config',
            });
            expect(mockPopup.close).toHaveBeenCalled();
        });

        it('should handle fetch error without detail', async () => {
            vi.spyOn(global, 'fetch').mockResolvedValue({
                ok: false,
                status: 500,
                json: () => Promise.reject(new Error('Invalid JSON')),
            } as Response);

            const { result } = renderHook(() => useTestOAuthConnection());

            await act(async () => {
                await result.current.testConnection(createFormState());
            });

            expect(mockNotification.error).toHaveBeenCalledWith({
                message: 'Test Connection Failed',
                description: 'Test connection failed: 500',
            });
        });

        it('should handle missing authorization URL', async () => {
            vi.spyOn(global, 'fetch').mockResolvedValue({
                ok: true,
                json: () => Promise.resolve({}),
            } as Response);

            const { result } = renderHook(() => useTestOAuthConnection());

            await act(async () => {
                await result.current.testConnection(createFormState());
            });

            expect(mockNotification.error).toHaveBeenCalledWith({
                message: 'Test Connection Failed',
                description: 'No authorization URL returned from server',
            });
        });

        it('should handle network error', async () => {
            vi.spyOn(global, 'fetch').mockRejectedValue(new Error('Network error'));

            const { result } = renderHook(() => useTestOAuthConnection());

            await act(async () => {
                await result.current.testConnection(createFormState());
            });

            expect(mockNotification.error).toHaveBeenCalledWith({
                message: 'Test Connection Failed',
                description: 'Network error',
            });
        });

        it('should handle non-Error thrown', async () => {
            vi.spyOn(global, 'fetch').mockRejectedValue('string error');

            const { result } = renderHook(() => useTestOAuthConnection());

            await act(async () => {
                await result.current.testConnection(createFormState());
            });

            expect(mockNotification.error).toHaveBeenCalledWith({
                message: 'Test Connection Failed',
                description: 'Please try again.',
            });
        });

        it('should close blank popup on error', async () => {
            vi.spyOn(global, 'fetch').mockRejectedValue(new Error('Failed'));

            const { result } = renderHook(() => useTestOAuthConnection());

            await act(async () => {
                await result.current.testConnection(createFormState());
            });

            expect(mockPopup.close).toHaveBeenCalled();
        });
    });

    describe('postMessage Result Handling', () => {
        it('should handle successful test result', () => {
            const { result } = renderHook(() => useTestOAuthConnection());

            act(() => {
                messageListeners.forEach((listener) =>
                    listener(
                        new MessageEvent('message', {
                            data: {
                                type: 'oauth_test_result',
                                success: true,
                                toolCount: 5,
                                toolNames: ['search', 'get', 'list', 'create', 'delete'],
                                message: 'Discovered 5 tools',
                            },
                        }),
                    ),
                );
            });

            expect(result.current.isTestingConnection).toBe(false);
            expect(result.current.testResult).toEqual({
                success: true,
                toolCount: 5,
                toolNames: ['search', 'get', 'list', 'create', 'delete'],
                message: 'Discovered 5 tools',
                error: undefined,
            });
        });

        it('should handle failed test result', () => {
            const { result } = renderHook(() => useTestOAuthConnection());

            act(() => {
                messageListeners.forEach((listener) =>
                    listener(
                        new MessageEvent('message', {
                            data: {
                                type: 'oauth_test_result',
                                success: false,
                                error: 'MCP server returned 401',
                            },
                        }),
                    ),
                );
            });

            expect(result.current.testResult).toEqual({
                success: false,
                toolCount: undefined,
                toolNames: undefined,
                message: undefined,
                error: 'MCP server returned 401',
            });
        });

        it('should ignore messages with different type', () => {
            const { result } = renderHook(() => useTestOAuthConnection());

            act(() => {
                messageListeners.forEach((listener) =>
                    listener(
                        new MessageEvent('message', {
                            data: { type: 'oauth_callback', success: true },
                        }),
                    ),
                );
            });

            expect(result.current.testResult).toBeNull();
        });

        it('should ignore messages with no data', () => {
            const { result } = renderHook(() => useTestOAuthConnection());

            act(() => {
                messageListeners.forEach((listener) => listener(new MessageEvent('message', { data: null })));
            });

            expect(result.current.testResult).toBeNull();
        });

        it('should close popup when result is received', async () => {
            const { result } = renderHook(() => useTestOAuthConnection());

            // Open popup first
            await act(async () => {
                await result.current.testConnection(createFormState());
            });

            act(() => {
                messageListeners.forEach((listener) =>
                    listener(
                        new MessageEvent('message', {
                            data: { type: 'oauth_test_result', success: true, toolCount: 3 },
                        }),
                    ),
                );
            });

            expect(mockPopup.close).toHaveBeenCalled();
        });
    });

    describe('Popup Close Detection', () => {
        it('should reset state when popup is closed by user', async () => {
            const { result } = renderHook(() => useTestOAuthConnection());

            await act(async () => {
                await result.current.testConnection(createFormState());
            });

            expect(result.current.isTestingConnection).toBe(true);

            mockPopup.closed = true;

            // Advance interval (500ms) + timeout (2000ms)
            await act(async () => {
                vi.advanceTimersByTime(500);
            });
            await act(async () => {
                vi.advanceTimersByTime(2000);
            });

            expect(result.current.isTestingConnection).toBe(false);
        });

        it('should not reset if result arrived before timeout', async () => {
            const { result } = renderHook(() => useTestOAuthConnection());

            await act(async () => {
                await result.current.testConnection(createFormState());
            });

            // Receive result via postMessage
            act(() => {
                messageListeners.forEach((listener) =>
                    listener(
                        new MessageEvent('message', {
                            data: { type: 'oauth_test_result', success: true, toolCount: 3 },
                        }),
                    ),
                );
            });

            // Now close popup and advance timers
            mockPopup.closed = true;
            await act(async () => {
                vi.advanceTimersByTime(3000);
            });

            // Should still have the result (not reset)
            expect(result.current.testResult).not.toBeNull();
            expect(result.current.testResult?.success).toBe(true);
        });
    });

    describe('clearTestResult', () => {
        it('should clear test result', () => {
            const { result } = renderHook(() => useTestOAuthConnection());

            // Set a result via postMessage
            act(() => {
                messageListeners.forEach((listener) =>
                    listener(
                        new MessageEvent('message', {
                            data: { type: 'oauth_test_result', success: true, toolCount: 3 },
                        }),
                    ),
                );
            });

            expect(result.current.testResult).not.toBeNull();

            act(() => {
                result.current.clearTestResult();
            });

            expect(result.current.testResult).toBeNull();
            expect(result.current.isTestingConnection).toBe(false);
        });
    });

    describe('Cleanup', () => {
        it('should close popup on unmount', async () => {
            const { result, unmount } = renderHook(() => useTestOAuthConnection());

            await act(async () => {
                await result.current.testConnection(createFormState());
            });

            unmount();

            expect(mockPopup.close).toHaveBeenCalled();
        });

        it('should clear interval on unmount', async () => {
            const clearIntervalSpy = vi.spyOn(global, 'clearInterval');
            const { result, unmount } = renderHook(() => useTestOAuthConnection());

            await act(async () => {
                await result.current.testConnection(createFormState());
            });

            unmount();

            expect(clearIntervalSpy).toHaveBeenCalled();
        });

        it('should not throw if no popup on unmount', () => {
            const { unmount } = renderHook(() => useTestOAuthConnection());
            expect(() => unmount()).not.toThrow();
        });
    });
});
