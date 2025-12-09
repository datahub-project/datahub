import { waitFor } from '@testing-library/react';
import { act, renderHook } from '@testing-library/react-hooks';
import { vi } from 'vitest';

import { useChatStream } from '@app/chat/hooks/useChatStream';

import { DataHubAiConversationActorType, DataHubAiConversationMessageType } from '@types';

// Mock fetch globally
const mockFetch = vi.fn();
global.fetch = mockFetch;

// Mock analytics
vi.mock('@app/analytics', () => ({
    default: {
        event: vi.fn(),
    },
    EventType: {
        DataHubChatResponseErrorEvent: 'DataHubChatResponseErrorEvent',
    },
}));

// Mock console methods to avoid noise in tests
const originalConsoleError = console.error;
const originalConsoleLog = console.log;

describe('useChatStream', () => {
    const mockConversationUrn = 'urn:li:agentConversation:test-conversation';
    const mockOnMessageReceived = vi.fn();
    const mockOnStreamComplete = vi.fn();

    beforeEach(() => {
        vi.clearAllMocks();
        mockFetch.mockClear();

        // Suppress console output during tests
        console.error = vi.fn();
        console.log = vi.fn();
    });

    afterEach(() => {
        // Restore console methods
        console.error = originalConsoleError;
        console.log = originalConsoleLog;
    });

    it('should initialize with default state', () => {
        const { result } = renderHook(() =>
            useChatStream({
                conversationUrn: mockConversationUrn,
                onMessageReceived: mockOnMessageReceived,
                onStreamComplete: mockOnStreamComplete,
            }),
        );

        expect(result.current.isStreaming).toBe(false);
        expect(result.current.currentMessage).toBeNull();
        expect(result.current.error).toBeNull();
        expect(typeof result.current.sendMessage).toBe('function');
        expect(typeof result.current.stopStreaming).toBe('function');
    });

    it('should handle successful message sending and streaming', async () => {
        const mockResponse = {
            ok: true,
            body: {
                getReader: vi.fn().mockReturnValue({
                    read: vi
                        .fn()
                        .mockResolvedValueOnce({
                            done: false,
                            value: new TextEncoder().encode(
                                'event: message\ndata: {"conversation_urn":"urn:li:agentConversation:test-conversation","message":{"type":"text","content":{"text":"Hello"}}}\n\n',
                            ),
                        })
                        .mockResolvedValueOnce({
                            done: false,
                            value: new TextEncoder().encode(
                                'event: message\ndata: {"conversation_urn":"urn:li:agentConversation:test-conversation","message":{"type":"text","content":{"text":" world"}}}\n\n',
                            ),
                        })
                        .mockResolvedValueOnce({
                            done: false,
                            value: new TextEncoder().encode(
                                'event: complete\ndata: {"conversation_urn":"urn:li:agentConversation:test-conversation"}\n\n',
                            ),
                        })
                        .mockResolvedValueOnce({
                            done: true,
                            value: undefined,
                        }),
                }),
            },
        };

        mockFetch.mockResolvedValueOnce(mockResponse);

        const { result } = renderHook(() =>
            useChatStream({
                conversationUrn: mockConversationUrn,
                onMessageReceived: mockOnMessageReceived,
                onStreamComplete: mockOnStreamComplete,
            }),
        );

        await act(async () => {
            await result.current.sendMessage('Hello, how are you?');
        });

        expect(mockFetch).toHaveBeenCalledWith('/openapi/v1/ai-chat/message', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                conversationUrn: mockConversationUrn,
                text: 'Hello, how are you?',
            }),
            signal: expect.any(AbortSignal),
        });

        await waitFor(() => {
            expect(result.current.isStreaming).toBe(false);
        });

        expect(mockOnMessageReceived).toHaveBeenCalled();
        expect(mockOnStreamComplete).toHaveBeenCalled();
    });

    it('should handle HTTP errors', async () => {
        const mockResponse = {
            ok: false,
            status: 500,
        };

        mockFetch.mockResolvedValueOnce(mockResponse);

        const { result } = renderHook(() =>
            useChatStream({
                conversationUrn: mockConversationUrn,
                onMessageReceived: mockOnMessageReceived,
                onStreamComplete: mockOnStreamComplete,
            }),
        );

        await act(async () => {
            await result.current.sendMessage('Test message');
        });

        await waitFor(() => {
            expect(result.current.isStreaming).toBe(false);
            expect(result.current.error).toBe('HTTP error! status: 500');
        });

        expect(mockOnMessageReceived).toHaveBeenCalledWith(
            expect.objectContaining({
                type: DataHubAiConversationMessageType.Text,
                content: {
                    text: 'Oops! An unexpected error occurred. 🥹 Please try again in a little while.',
                },
            }),
        );
    });

    it('should handle network errors', async () => {
        const networkError = new Error('Network error');
        mockFetch.mockRejectedValueOnce(networkError);

        const { result } = renderHook(() =>
            useChatStream({
                conversationUrn: mockConversationUrn,
                onMessageReceived: mockOnMessageReceived,
                onStreamComplete: mockOnStreamComplete,
            }),
        );

        await act(async () => {
            await result.current.sendMessage('Test message');
        });

        await waitFor(() => {
            expect(result.current.isStreaming).toBe(false);
            expect(result.current.error).toBe('Network error');
        });

        expect(mockOnMessageReceived).toHaveBeenCalledWith(
            expect.objectContaining({
                type: DataHubAiConversationMessageType.Text,
                content: {
                    text: 'Oops! An unexpected error occurred. 🥹 Please try again in a little while.',
                },
            }),
        );
    });

    it('should handle stream errors', async () => {
        const mockResponse = {
            ok: true,
            body: {
                getReader: vi.fn().mockReturnValue({
                    read: vi
                        .fn()
                        .mockResolvedValueOnce({
                            done: false,
                            value: new TextEncoder().encode('event:error\n\n'),
                        })
                        .mockResolvedValueOnce({
                            done: true,
                            value: undefined,
                        }),
                }),
            },
        };

        mockFetch.mockResolvedValueOnce(mockResponse);

        const { result } = renderHook(() =>
            useChatStream({
                conversationUrn: mockConversationUrn,
                onMessageReceived: mockOnMessageReceived,
                onStreamComplete: mockOnStreamComplete,
            }),
        );

        await act(async () => {
            await result.current.sendMessage('Test message');
        });

        await waitFor(() => {
            expect(result.current.isStreaming).toBe(false);
        });

        expect(mockOnMessageReceived).toHaveBeenCalledWith(
            expect.objectContaining({
                type: DataHubAiConversationMessageType.Text,
                content: {
                    text: 'Oops! An unexpected error occurred. 🥹 Please try again in a little while.',
                },
            }),
        );
    });

    it('should handle abort errors gracefully', async () => {
        const abortError = new Error('Aborted');
        abortError.name = 'AbortError';
        mockFetch.mockRejectedValueOnce(abortError);

        const { result } = renderHook(() =>
            useChatStream({
                conversationUrn: mockConversationUrn,
                onMessageReceived: mockOnMessageReceived,
                onStreamComplete: mockOnStreamComplete,
            }),
        );

        await act(async () => {
            await result.current.sendMessage('Test message');
        });

        // For abort errors, the hook should handle it gracefully without setting error state
        expect(result.current.isStreaming).toBe(false);
        expect(result.current.error).toBeNull();

        // Should not call onMessageReceived for abort errors
        expect(mockOnMessageReceived).not.toHaveBeenCalled();
    });

    it('should handle connection interrupted errors gracefully', async () => {
        const connectionError = new Error('Connection interrupted');
        mockFetch.mockRejectedValueOnce(connectionError);

        const { result } = renderHook(() =>
            useChatStream({
                conversationUrn: mockConversationUrn,
                onMessageReceived: mockOnMessageReceived,
                onStreamComplete: mockOnStreamComplete,
            }),
        );

        await act(async () => {
            await result.current.sendMessage('Test message');
        });

        // For connection interrupted errors (user clicked stop), should handle gracefully
        expect(result.current.isStreaming).toBe(false);
        expect(result.current.error).toBeNull();

        // Should not call onMessageReceived for connection interrupted errors
        expect(mockOnMessageReceived).not.toHaveBeenCalled();
    });

    it('should stop streaming when stopStreaming is called', async () => {
        const mockResponse = {
            ok: true,
            body: {
                getReader: vi.fn().mockReturnValue({
                    read: vi.fn().mockImplementation(() => {
                        // Simulate a long-running stream
                        return new Promise((resolve) => {
                            setTimeout(() => {
                                resolve({
                                    done: false,
                                    value: new TextEncoder().encode(
                                        'data: {"type":"text","content":{"text":"Hello"}}\n\n',
                                    ),
                                });
                            }, 100);
                        });
                    }),
                }),
            },
        };

        mockFetch.mockResolvedValueOnce(mockResponse);

        const { result } = renderHook(() =>
            useChatStream({
                conversationUrn: mockConversationUrn,
                onMessageReceived: mockOnMessageReceived,
                onStreamComplete: mockOnStreamComplete,
            }),
        );

        act(() => {
            result.current.sendMessage('Test message');
        });

        // Wait a bit for the stream to start
        await new Promise((resolve) => {
            setTimeout(resolve, 50);
        });

        act(() => {
            result.current.stopStreaming();
        });

        expect(result.current.isStreaming).toBe(false);
        expect(result.current.currentMessage).toBeNull();
    });

    it('should process message queue correctly', async () => {
        const createMockResponse = () => ({
            ok: true,
            body: {
                getReader: vi.fn().mockReturnValue({
                    read: vi
                        .fn()
                        .mockResolvedValueOnce({
                            done: false,
                            value: new TextEncoder().encode('data: {"type":"complete"}\n\n'),
                        })
                        .mockResolvedValueOnce({
                            done: true,
                            value: undefined,
                        }),
                }),
            },
        });

        mockFetch
            .mockResolvedValueOnce(createMockResponse())
            .mockResolvedValueOnce(createMockResponse())
            .mockResolvedValueOnce(createMockResponse());

        const { result } = renderHook(() =>
            useChatStream({
                conversationUrn: mockConversationUrn,
                onMessageReceived: mockOnMessageReceived,
                onStreamComplete: mockOnStreamComplete,
            }),
        );

        // Send multiple messages quickly
        act(() => {
            result.current.sendMessage('Message 1');
            result.current.sendMessage('Message 2');
            result.current.sendMessage('Message 3');
        });

        await waitFor(() => {
            expect(mockFetch).toHaveBeenCalledTimes(3);
        });

        expect(mockOnStreamComplete).toHaveBeenCalledTimes(3);
    });

    it('should handle different message types in stream', async () => {
        const mockResponse = {
            ok: true,
            body: {
                getReader: vi.fn().mockReturnValue({
                    read: vi
                        .fn()
                        .mockResolvedValueOnce({
                            done: false,
                            value: new TextEncoder().encode(
                                'event: message\ndata: {"conversation_urn":"urn:li:agentConversation:test-conversation","message":{"type":"thinking","content":{"text":"Let me think..."}}}\n\n',
                            ),
                        })
                        .mockResolvedValueOnce({
                            done: false,
                            value: new TextEncoder().encode(
                                'event: message\ndata: {"conversation_urn":"urn:li:agentConversation:test-conversation","message":{"type":"text","content":{"text":"Hello"}}}\n\n',
                            ),
                        })
                        .mockResolvedValueOnce({
                            done: false,
                            value: new TextEncoder().encode(
                                'event: message\ndata: {"conversation_urn":"urn:li:agentConversation:test-conversation","message":{"type":"text","content":{"text":" world"}}}\n\n',
                            ),
                        })
                        .mockResolvedValueOnce({
                            done: false,
                            value: new TextEncoder().encode(
                                'event: complete\ndata: {"conversation_urn":"urn:li:agentConversation:test-conversation"}\n\n',
                            ),
                        })
                        .mockResolvedValueOnce({
                            done: true,
                            value: undefined,
                        }),
                }),
            },
        };

        mockFetch.mockResolvedValueOnce(mockResponse);

        const { result } = renderHook(() =>
            useChatStream({
                conversationUrn: mockConversationUrn,
                onMessageReceived: mockOnMessageReceived,
                onStreamComplete: mockOnStreamComplete,
            }),
        );

        await act(async () => {
            await result.current.sendMessage('Test message');
        });

        await waitFor(() => {
            expect(result.current.isStreaming).toBe(false);
        });

        // Should receive the thinking message and the text messages
        expect(mockOnMessageReceived).toHaveBeenCalledWith(
            expect.objectContaining({
                type: 'thinking',
                content: { text: 'Let me think...' },
            }),
        );

        // Should receive individual text chunks
        expect(mockOnMessageReceived).toHaveBeenCalledWith(
            expect.objectContaining({
                type: 'text',
                actor: { type: DataHubAiConversationActorType.Agent },
                content: { text: 'Hello' },
            }),
        );

        expect(mockOnMessageReceived).toHaveBeenCalledWith(
            expect.objectContaining({
                type: 'text',
                actor: { type: DataHubAiConversationActorType.Agent },
                content: { text: ' world' },
            }),
        );
    });

    it('should handle malformed JSON in stream', async () => {
        const mockResponse = {
            ok: true,
            body: {
                getReader: vi.fn().mockReturnValue({
                    read: vi
                        .fn()
                        .mockResolvedValueOnce({
                            done: false,
                            value: new TextEncoder().encode('data: invalid json\n\n'),
                        })
                        .mockResolvedValueOnce({
                            done: false,
                            value: new TextEncoder().encode('data: {"type":"complete"}\n\n'),
                        })
                        .mockResolvedValueOnce({
                            done: true,
                            value: undefined,
                        }),
                }),
            },
        };

        mockFetch.mockResolvedValueOnce(mockResponse);

        const { result } = renderHook(() =>
            useChatStream({
                conversationUrn: mockConversationUrn,
                onMessageReceived: mockOnMessageReceived,
                onStreamComplete: mockOnStreamComplete,
            }),
        );

        await act(async () => {
            await result.current.sendMessage('Test message');
        });

        await waitFor(() => {
            expect(result.current.isStreaming).toBe(false);
        });

        expect(mockOnStreamComplete).toHaveBeenCalled();
    });

    it('should handle error data in stream', async () => {
        const mockResponse = {
            ok: true,
            body: {
                getReader: vi.fn().mockReturnValue({
                    read: vi
                        .fn()
                        .mockResolvedValueOnce({
                            done: false,
                            value: new TextEncoder().encode('data: {"error":"Something went wrong"}\n\n'),
                        })
                        .mockResolvedValueOnce({
                            done: true,
                            value: undefined,
                        }),
                }),
            },
        };

        mockFetch.mockResolvedValueOnce(mockResponse);

        const { result } = renderHook(() =>
            useChatStream({
                conversationUrn: mockConversationUrn,
                onMessageReceived: mockOnMessageReceived,
                onStreamComplete: mockOnStreamComplete,
            }),
        );

        await act(async () => {
            await result.current.sendMessage('Test message');
        });

        await waitFor(() => {
            expect(result.current.isStreaming).toBe(false);
        });

        expect(mockOnMessageReceived).toHaveBeenCalledWith(
            expect.objectContaining({
                type: DataHubAiConversationMessageType.Text,
                content: {
                    text: 'Oops! An unexpected error occurred. 🥹 Please try again in a little while.',
                },
            }),
        );
    });

    it('should handle empty stream response', async () => {
        const mockResponse = {
            ok: true,
            body: {
                getReader: vi.fn().mockReturnValue({
                    read: vi.fn().mockResolvedValueOnce({
                        done: true,
                        value: undefined,
                    }),
                }),
            },
        };

        mockFetch.mockResolvedValueOnce(mockResponse);

        const { result } = renderHook(() =>
            useChatStream({
                conversationUrn: mockConversationUrn,
                onMessageReceived: mockOnMessageReceived,
                onStreamComplete: mockOnStreamComplete,
            }),
        );

        await act(async () => {
            await result.current.sendMessage('Test message');
        });

        await waitFor(() => {
            expect(result.current.isStreaming).toBe(false);
        });

        expect(mockOnStreamComplete).toHaveBeenCalled();
    });

    it('should handle missing response body', async () => {
        const mockResponse = {
            ok: true,
            body: null,
        };

        mockFetch.mockResolvedValueOnce(mockResponse);

        const { result } = renderHook(() =>
            useChatStream({
                conversationUrn: mockConversationUrn,
                onMessageReceived: mockOnMessageReceived,
                onStreamComplete: mockOnStreamComplete,
            }),
        );

        await act(async () => {
            await result.current.sendMessage('Test message');
        });

        await waitFor(() => {
            expect(result.current.isStreaming).toBe(false);
            expect(result.current.error).toBe('No reader available');
        });

        expect(mockOnMessageReceived).toHaveBeenCalledWith(
            expect.objectContaining({
                type: DataHubAiConversationMessageType.Text,
                content: {
                    text: 'Oops! An unexpected error occurred. 🥹 Please try again in a little while.',
                },
            }),
        );
    });

    it('should include agentName in request body when provided', async () => {
        const mockResponse = {
            ok: true,
            body: {
                getReader: vi.fn().mockReturnValue({
                    read: vi
                        .fn()
                        .mockResolvedValueOnce({
                            done: false,
                            value: new TextEncoder().encode('event: complete\ndata: {}\n\n'),
                        })
                        .mockResolvedValueOnce({
                            done: true,
                            value: undefined,
                        }),
                }),
            },
        };

        mockFetch.mockResolvedValueOnce(mockResponse);

        const { result } = renderHook(() =>
            useChatStream({
                conversationUrn: mockConversationUrn,
                onMessageReceived: mockOnMessageReceived,
                onStreamComplete: mockOnStreamComplete,
                agentName: 'custom-agent',
            }),
        );

        await act(async () => {
            await result.current.sendMessage('Test message');
        });

        expect(mockFetch).toHaveBeenCalledWith('/openapi/v1/ai-chat/message', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                conversationUrn: mockConversationUrn,
                text: 'Test message',
                agentName: 'custom-agent',
            }),
            signal: expect.any(AbortSignal),
        });
    });

    it('should not include agentName in request body when not provided', async () => {
        const mockResponse = {
            ok: true,
            body: {
                getReader: vi.fn().mockReturnValue({
                    read: vi
                        .fn()
                        .mockResolvedValueOnce({
                            done: false,
                            value: new TextEncoder().encode('event: complete\ndata: {}\n\n'),
                        })
                        .mockResolvedValueOnce({
                            done: true,
                            value: undefined,
                        }),
                }),
            },
        };

        mockFetch.mockResolvedValueOnce(mockResponse);

        const { result } = renderHook(() =>
            useChatStream({
                conversationUrn: mockConversationUrn,
                onMessageReceived: mockOnMessageReceived,
                onStreamComplete: mockOnStreamComplete,
            }),
        );

        await act(async () => {
            await result.current.sendMessage('Test message');
        });

        const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
        expect(requestBody).toEqual({
            conversationUrn: mockConversationUrn,
            text: 'Test message',
            agentName: undefined,
        });
    });
});
