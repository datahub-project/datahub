import { act, renderHook } from '@testing-library/react-hooks';
import { message } from 'antd';
import { vi } from 'vitest';

import { useOAuthConnect } from '@app/settingsV2/personal/aiConnections/useOAuthConnect';

// Mock console.log to avoid noise in tests
const mockConsoleLog = vi.spyOn(console, 'log').mockImplementation(() => {});

// Mock Ant Design message
vi.mock('antd', () => ({
    message: {
        success: vi.fn(),
        error: vi.fn(),
    },
}));

describe('useOAuthConnect', () => {
    const mockOnSuccess = vi.fn();
    let messageListeners: ((event: MessageEvent) => void)[] = [];
    let mockPopup: { closed: boolean; close: ReturnType<typeof vi.fn> };

    beforeEach(() => {
        vi.clearAllMocks();
        vi.useFakeTimers();
        mockConsoleLog.mockClear();
        messageListeners = [];

        // Mock window.addEventListener for 'message' events
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

        // Mock popup window
        mockPopup = { closed: false, close: vi.fn() };
        vi.spyOn(window, 'open').mockReturnValue(mockPopup as unknown as Window);

        // Mock fetch
        vi.spyOn(global, 'fetch').mockResolvedValue({
            ok: true,
            json: () => Promise.resolve({ authorization_url: 'https://oauth.example.com/authorize' }),
        } as Response);
    });

    afterEach(() => {
        vi.useRealTimers();
        vi.restoreAllMocks();
    });

    afterAll(() => {
        mockConsoleLog.mockRestore();
    });

    describe('Initialization', () => {
        it('should initialize with isConnecting false', () => {
            const { result } = renderHook(() => useOAuthConnect());

            expect(result.current.isConnecting).toBe(false);
            expect(typeof result.current.initiateOAuthConnect).toBe('function');
        });

        it('should register message event listener on mount', () => {
            renderHook(() => useOAuthConnect());

            expect(window.addEventListener).toHaveBeenCalledWith('message', expect.any(Function));
        });

        it('should remove message event listener on unmount', () => {
            const { unmount } = renderHook(() => useOAuthConnect());

            unmount();

            expect(window.removeEventListener).toHaveBeenCalledWith('message', expect.any(Function));
        });
    });

    describe('OAuth Message Handling', () => {
        it('should log received postMessage', () => {
            // Create a fresh spy for this test
            const consoleSpy = vi.spyOn(console, 'log');

            renderHook(() => useOAuthConnect());

            const messageEvent = new MessageEvent('message', {
                data: {
                    type: 'oauth_callback',
                    success: true,
                    pluginId: 'test-plugin',
                },
                origin: 'https://example.com',
            });

            act(() => {
                messageListeners.forEach((listener) => listener(messageEvent));
            });

            // Verify the log was called with the expected message
            expect(consoleSpy).toHaveBeenCalledWith(
                '[OAuth] Received postMessage:',
                expect.objectContaining({
                    type: 'oauth_callback',
                    success: true,
                    pluginId: 'test-plugin',
                    origin: 'https://example.com',
                }),
            );

            consoleSpy.mockRestore();
        });

        it('should handle successful oauth_callback message', async () => {
            const { result } = renderHook(() => useOAuthConnect(mockOnSuccess));

            // Simulate receiving a successful OAuth callback
            const messageEvent = new MessageEvent('message', {
                data: {
                    type: 'oauth_callback',
                    success: true,
                    pluginId: 'test-plugin',
                },
            });

            act(() => {
                messageListeners.forEach((listener) => listener(messageEvent));
            });

            expect(result.current.isConnecting).toBe(false);
            expect(message.success).toHaveBeenCalledWith('Connected to test-plugin successfully!');

            // Fast-forward the 500ms delay for onSuccess
            await act(async () => {
                vi.advanceTimersByTime(500);
            });

            expect(mockOnSuccess).toHaveBeenCalled();
        });

        it('should handle successful oauth_callback without pluginId', () => {
            const { result } = renderHook(() => useOAuthConnect());

            const messageEvent = new MessageEvent('message', {
                data: {
                    type: 'oauth_callback',
                    success: true,
                },
            });

            act(() => {
                messageListeners.forEach((listener) => listener(messageEvent));
            });

            expect(result.current.isConnecting).toBe(false);
            expect(message.success).toHaveBeenCalledWith('Connected to plugin successfully!');
        });

        it('should handle failed oauth_callback message with error', () => {
            const { result } = renderHook(() => useOAuthConnect(mockOnSuccess));

            const messageEvent = new MessageEvent('message', {
                data: {
                    type: 'oauth_callback',
                    success: false,
                    error: 'User denied access',
                },
            });

            act(() => {
                messageListeners.forEach((listener) => listener(messageEvent));
            });

            expect(result.current.isConnecting).toBe(false);
            expect(message.error).toHaveBeenCalledWith('User denied access');
            expect(mockOnSuccess).not.toHaveBeenCalled();
        });

        it('should handle failed oauth_callback message without error', () => {
            const { result } = renderHook(() => useOAuthConnect());

            const messageEvent = new MessageEvent('message', {
                data: {
                    type: 'oauth_callback',
                    success: false,
                },
            });

            act(() => {
                messageListeners.forEach((listener) => listener(messageEvent));
            });

            expect(result.current.isConnecting).toBe(false);
            expect(message.error).toHaveBeenCalledWith('Failed to connect. Please try again.');
        });

        it('should ignore messages with different type', () => {
            renderHook(() => useOAuthConnect(mockOnSuccess));

            const messageEvent = new MessageEvent('message', {
                data: {
                    type: 'other_type',
                    success: true,
                },
            });

            act(() => {
                messageListeners.forEach((listener) => listener(messageEvent));
            });

            // Should not call success/error or onSuccess
            expect(message.success).not.toHaveBeenCalled();
            expect(message.error).not.toHaveBeenCalled();
            expect(mockOnSuccess).not.toHaveBeenCalled();
        });

        it('should ignore messages with no data', () => {
            renderHook(() => useOAuthConnect(mockOnSuccess));

            const messageEvent = new MessageEvent('message', {
                data: null,
            });

            act(() => {
                messageListeners.forEach((listener) => listener(messageEvent));
            });

            expect(message.success).not.toHaveBeenCalled();
            expect(message.error).not.toHaveBeenCalled();
        });

        it('should close popup when receiving oauth_callback', async () => {
            const { result: hookResult } = renderHook(() => useOAuthConnect());

            // First initiate a connection to set the popup ref
            await act(async () => {
                await hookResult.current.initiateOAuthConnect('test-plugin');
            });

            const messageEvent = new MessageEvent('message', {
                data: {
                    type: 'oauth_callback',
                    success: true,
                    pluginId: 'test-plugin',
                },
            });

            act(() => {
                messageListeners.forEach((listener) => listener(messageEvent));
            });

            expect(mockPopup.close).toHaveBeenCalled();
        });
    });

    describe('initiateOAuthConnect', () => {
        it('should set isConnecting to true when called', async () => {
            const { result } = renderHook(() => useOAuthConnect());

            expect(result.current.isConnecting).toBe(false);

            await act(async () => {
                result.current.initiateOAuthConnect('test-plugin');
            });

            expect(result.current.isConnecting).toBe(true);
        });

        it('should call fetch with correct URL and options', async () => {
            const { result } = renderHook(() => useOAuthConnect());

            await act(async () => {
                await result.current.initiateOAuthConnect('test-plugin');
            });

            expect(fetch).toHaveBeenCalledWith('/integrations/oauth/plugins/test-plugin/connect', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                credentials: 'include',
            });
        });

        it('should encode pluginId in URL', async () => {
            const { result } = renderHook(() => useOAuthConnect());

            await act(async () => {
                await result.current.initiateOAuthConnect('plugin with spaces');
            });

            expect(fetch).toHaveBeenCalledWith(
                '/integrations/oauth/plugins/plugin%20with%20spaces/connect',
                expect.any(Object),
            );
        });

        it('should open popup with authorization URL', async () => {
            const { result } = renderHook(() => useOAuthConnect());

            await act(async () => {
                await result.current.initiateOAuthConnect('test-plugin');
            });

            expect(window.open).toHaveBeenCalledWith(
                'https://oauth.example.com/authorize',
                'oauth_connect',
                expect.stringContaining('width=600,height=700'),
            );
        });

        it('should handle fetch error with error response', async () => {
            vi.spyOn(global, 'fetch').mockResolvedValue({
                ok: false,
                status: 400,
                json: () => Promise.resolve({ detail: 'Plugin not found' }),
            } as Response);

            const { result } = renderHook(() => useOAuthConnect());

            await act(async () => {
                await result.current.initiateOAuthConnect('invalid-plugin');
            });

            expect(result.current.isConnecting).toBe(false);
            expect(message.error).toHaveBeenCalledWith('Plugin not found');
        });

        it('should handle fetch error without error response', async () => {
            vi.spyOn(global, 'fetch').mockResolvedValue({
                ok: false,
                status: 500,
                json: () => Promise.reject(new Error('Invalid JSON')),
            } as Response);

            const { result } = renderHook(() => useOAuthConnect());

            await act(async () => {
                await result.current.initiateOAuthConnect('test-plugin');
            });

            expect(result.current.isConnecting).toBe(false);
            expect(message.error).toHaveBeenCalledWith('Failed to initiate OAuth: 500');
        });

        it('should handle missing authorization URL', async () => {
            vi.spyOn(global, 'fetch').mockResolvedValue({
                ok: true,
                json: () => Promise.resolve({}),
            } as Response);

            const { result } = renderHook(() => useOAuthConnect());

            await act(async () => {
                await result.current.initiateOAuthConnect('test-plugin');
            });

            expect(result.current.isConnecting).toBe(false);
            expect(message.error).toHaveBeenCalledWith('No authorization URL returned from server');
        });

        it('should handle popup blocked', async () => {
            vi.spyOn(window, 'open').mockReturnValue(null);

            const { result } = renderHook(() => useOAuthConnect());

            await act(async () => {
                await result.current.initiateOAuthConnect('test-plugin');
            });

            expect(result.current.isConnecting).toBe(false);
            expect(message.error).toHaveBeenCalledWith('Failed to open popup. Please allow popups for this site.');
        });

        it('should handle network error', async () => {
            vi.spyOn(global, 'fetch').mockRejectedValue(new Error('Network error'));

            const { result } = renderHook(() => useOAuthConnect());

            await act(async () => {
                await result.current.initiateOAuthConnect('test-plugin');
            });

            expect(result.current.isConnecting).toBe(false);
            expect(message.error).toHaveBeenCalledWith('Network error');
        });

        it('should handle non-Error thrown', async () => {
            vi.spyOn(global, 'fetch').mockRejectedValue('string error');

            const { result } = renderHook(() => useOAuthConnect());

            await act(async () => {
                await result.current.initiateOAuthConnect('test-plugin');
            });

            expect(result.current.isConnecting).toBe(false);
            expect(message.error).toHaveBeenCalledWith('Failed to connect. Please try again.');
        });
    });

    describe('Popup Closed Detection', () => {
        it('should reset isConnecting when popup is closed by user', async () => {
            const { result } = renderHook(() => useOAuthConnect());

            await act(async () => {
                await result.current.initiateOAuthConnect('test-plugin');
            });

            expect(result.current.isConnecting).toBe(true);

            // Simulate popup being closed
            mockPopup.closed = true;

            // Advance timer to trigger the interval check
            await act(async () => {
                vi.advanceTimersByTime(500);
            });

            expect(result.current.isConnecting).toBe(false);
        });
    });

    describe('Cleanup', () => {
        it('should close popup on unmount if still open', async () => {
            const { result, unmount } = renderHook(() => useOAuthConnect());

            await act(async () => {
                await result.current.initiateOAuthConnect('test-plugin');
            });

            unmount();

            expect(mockPopup.close).toHaveBeenCalled();
        });

        it('should not throw if popup already closed on unmount', async () => {
            const { result, unmount } = renderHook(() => useOAuthConnect());

            await act(async () => {
                await result.current.initiateOAuthConnect('test-plugin');
            });

            mockPopup.closed = true;

            expect(() => unmount()).not.toThrow();
        });

        it('should not close popup on unmount if no popup was opened', () => {
            const { unmount } = renderHook(() => useOAuthConnect());

            // Just unmount without initiating any connection
            expect(() => unmount()).not.toThrow();
            expect(mockPopup.close).not.toHaveBeenCalled();
        });
    });

    describe('Popup Position Calculation', () => {
        it('should calculate popup position based on window properties', async () => {
            // Mock window properties for position calculation
            Object.defineProperty(window, 'screenX', { value: 100, writable: true });
            Object.defineProperty(window, 'screenY', { value: 50, writable: true });
            Object.defineProperty(window, 'outerWidth', { value: 1200, writable: true });
            Object.defineProperty(window, 'outerHeight', { value: 800, writable: true });

            const { result } = renderHook(() => useOAuthConnect());

            await act(async () => {
                await result.current.initiateOAuthConnect('test-plugin');
            });

            // Expected: left = 100 + (1200 - 600) / 2 = 400
            // Expected: top = 50 + (800 - 700) / 2 = 100
            expect(window.open).toHaveBeenCalledWith(
                'https://oauth.example.com/authorize',
                'oauth_connect',
                expect.stringContaining('left=400'),
            );
            expect(window.open).toHaveBeenCalledWith(
                'https://oauth.example.com/authorize',
                'oauth_connect',
                expect.stringContaining('top=100'),
            );
        });

        it('should include all popup features in the window.open call', async () => {
            const { result } = renderHook(() => useOAuthConnect());

            await act(async () => {
                await result.current.initiateOAuthConnect('test-plugin');
            });

            const openCall = vi.mocked(window.open).mock.calls[0];
            const features = openCall[2];

            expect(features).toContain('width=600');
            expect(features).toContain('height=700');
            expect(features).toContain('toolbar=no');
            expect(features).toContain('menubar=no');
            expect(features).toContain('scrollbars=yes');
            expect(features).toContain('resizable=yes');
        });
    });

    describe('Multiple Connections', () => {
        it('should handle calling initiateOAuthConnect multiple times', async () => {
            const { result } = renderHook(() => useOAuthConnect());

            // First call
            await act(async () => {
                await result.current.initiateOAuthConnect('plugin-1');
            });

            expect(fetch).toHaveBeenCalledTimes(1);
            expect(window.open).toHaveBeenCalledTimes(1);

            // Second call
            await act(async () => {
                await result.current.initiateOAuthConnect('plugin-2');
            });

            expect(fetch).toHaveBeenCalledTimes(2);
            expect(window.open).toHaveBeenCalledTimes(2);
        });
    });

    describe('Interval Cleanup', () => {
        it('should clear interval when popup is detected as closed', async () => {
            const clearIntervalSpy = vi.spyOn(global, 'clearInterval');
            const { result } = renderHook(() => useOAuthConnect());

            await act(async () => {
                await result.current.initiateOAuthConnect('test-plugin');
            });

            // Popup is still open, interval should not be cleared
            await act(async () => {
                vi.advanceTimersByTime(500);
            });

            expect(result.current.isConnecting).toBe(true);

            // Now close the popup
            mockPopup.closed = true;

            await act(async () => {
                vi.advanceTimersByTime(500);
            });

            expect(result.current.isConnecting).toBe(false);
            expect(clearIntervalSpy).toHaveBeenCalled();
        });

        it('should keep checking while popup remains open', async () => {
            const { result } = renderHook(() => useOAuthConnect());

            await act(async () => {
                await result.current.initiateOAuthConnect('test-plugin');
            });

            // Advance multiple intervals while popup stays open (2500ms total)
            await act(async () => {
                vi.advanceTimersByTime(2500);
            });

            // Should still be connecting since popup is open
            expect(result.current.isConnecting).toBe(true);

            // Finally close the popup
            mockPopup.closed = true;

            await act(async () => {
                vi.advanceTimersByTime(500);
            });

            expect(result.current.isConnecting).toBe(false);
        });
    });

    describe('onSuccess Callback', () => {
        it('should work without onSuccess callback provided', async () => {
            const { result } = renderHook(() => useOAuthConnect()); // No callback

            const messageEvent = new MessageEvent('message', {
                data: {
                    type: 'oauth_callback',
                    success: true,
                    pluginId: 'test-plugin',
                },
            });

            act(() => {
                messageListeners.forEach((listener) => listener(messageEvent));
            });

            expect(result.current.isConnecting).toBe(false);
            expect(message.success).toHaveBeenCalled();

            // Advance timer - should not throw even without callback
            await act(async () => {
                vi.advanceTimersByTime(500);
            });

            // No error should occur
            expect(message.error).not.toHaveBeenCalled();
        });

        it('should call onSuccess with proper delay after success', async () => {
            renderHook(() => useOAuthConnect(mockOnSuccess));

            const messageEvent = new MessageEvent('message', {
                data: {
                    type: 'oauth_callback',
                    success: true,
                    pluginId: 'test-plugin',
                },
            });

            act(() => {
                messageListeners.forEach((listener) => listener(messageEvent));
            });

            // onSuccess should not be called immediately
            expect(mockOnSuccess).not.toHaveBeenCalled();

            // Advance 250ms - still not called
            await act(async () => {
                vi.advanceTimersByTime(250);
            });
            expect(mockOnSuccess).not.toHaveBeenCalled();

            // Advance another 250ms (total 500ms) - now called
            await act(async () => {
                vi.advanceTimersByTime(250);
            });
            expect(mockOnSuccess).toHaveBeenCalledTimes(1);
        });
    });

    describe('Message Handler Edge Cases', () => {
        it('should handle message with undefined data', () => {
            renderHook(() => useOAuthConnect(mockOnSuccess));

            const messageEvent = new MessageEvent('message', {
                data: undefined,
            });

            act(() => {
                messageListeners.forEach((listener) => listener(messageEvent));
            });

            expect(message.success).not.toHaveBeenCalled();
            expect(message.error).not.toHaveBeenCalled();
        });

        it('should handle oauth_callback while popup is already closed', async () => {
            const { result } = renderHook(() => useOAuthConnect());

            // Initiate connection and close popup
            await act(async () => {
                await result.current.initiateOAuthConnect('test-plugin');
            });

            mockPopup.closed = true;

            // Receive callback - should still process but not try to close again
            const messageEvent = new MessageEvent('message', {
                data: {
                    type: 'oauth_callback',
                    success: true,
                    pluginId: 'test-plugin',
                },
            });

            act(() => {
                messageListeners.forEach((listener) => listener(messageEvent));
            });

            expect(result.current.isConnecting).toBe(false);
            expect(message.success).toHaveBeenCalled();
            // close() should only be called once (during the callback processing check)
        });

        it('should handle oauth_callback when no popup was opened', () => {
            const { result } = renderHook(() => useOAuthConnect());

            // Receive callback without ever opening a popup
            const messageEvent = new MessageEvent('message', {
                data: {
                    type: 'oauth_callback',
                    success: true,
                    pluginId: 'test-plugin',
                },
            });

            act(() => {
                messageListeners.forEach((listener) => listener(messageEvent));
            });

            // Should still handle the message gracefully
            expect(result.current.isConnecting).toBe(false);
            expect(message.success).toHaveBeenCalled();
        });
    });

    describe('Hook Re-render', () => {
        it('should maintain state across re-renders', async () => {
            const { result, rerender } = renderHook(() => useOAuthConnect());

            await act(async () => {
                await result.current.initiateOAuthConnect('test-plugin');
            });

            expect(result.current.isConnecting).toBe(true);

            // Re-render the hook
            rerender();

            // State should be preserved
            expect(result.current.isConnecting).toBe(true);
        });

        it('should update message listener when onSuccess changes', () => {
            const onSuccess1 = vi.fn();
            const onSuccess2 = vi.fn();

            const { rerender } = renderHook(({ onSuccess }) => useOAuthConnect(onSuccess), {
                initialProps: { onSuccess: onSuccess1 },
            });

            // Re-render with new callback
            rerender({ onSuccess: onSuccess2 });

            // The message listener should have been updated
            // (This tests the useEffect dependency on onSuccess)
            expect(window.removeEventListener).toHaveBeenCalled();
            expect(window.addEventListener).toHaveBeenCalled();
        });
    });
});
