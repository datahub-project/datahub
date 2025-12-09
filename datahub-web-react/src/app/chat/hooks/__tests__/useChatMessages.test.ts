import { act, renderHook } from '@testing-library/react-hooks';
import { vi } from 'vitest';
import type { MockedFunction } from 'vitest';

import analytics from '@app/analytics';
import { useChatMessages } from '@app/chat/hooks/useChatMessages';
import { useChatStream } from '@app/chat/hooks/useChatStream';
import { createUserMessage, emitMessageAnalytics } from '@app/chat/utils/chatMessageUtils';
import { groupMessages } from '@app/chat/utils/messageGrouping';

import { DataHubAiConversationActorType, DataHubAiConversationMessageType } from '@types';
import type { DataHubAiConversationMessage } from '@types';

// Mock dependencies
vi.mock('@app/chat/hooks/useChatStream');
vi.mock('@app/chat/utils/chatMessageUtils');
vi.mock('@app/chat/utils/messageGrouping');
vi.mock('@app/analytics', () => ({
    default: {
        event: vi.fn(),
    },
    EventType: {
        CreateDataHubChatMessageEvent: 'CreateDataHubChatMessageEvent',
        StopDataHubChatResponseEvent: 'StopDataHubChatResponseEvent',
    },
}));

const mockUseChatStream = useChatStream as MockedFunction<typeof useChatStream>;
const mockCreateUserMessage = createUserMessage as MockedFunction<typeof createUserMessage>;
const mockEmitMessageAnalytics = emitMessageAnalytics as MockedFunction<typeof emitMessageAnalytics>;
const mockGroupMessages = groupMessages as MockedFunction<typeof groupMessages>;

describe('useChatMessages', () => {
    const mockConversationUrn = 'urn:li:agentConversation:test-conversation';
    const mockUserUrn = 'urn:li:corpuser:test-user';
    const mockSendMessage = vi.fn();
    const mockStopStreaming = vi.fn();
    const mockOnStreamComplete = vi.fn();
    const mockOnMessageReceived = vi.fn();

    const mockUserMessage: DataHubAiConversationMessage = {
        type: DataHubAiConversationMessageType.Text,
        time: Date.now(),
        actor: {
            type: DataHubAiConversationActorType.User,
            actor: mockUserUrn,
        },
        content: {
            text: 'Test message',
        },
    };

    const mockAgentMessage: DataHubAiConversationMessage = {
        type: DataHubAiConversationMessageType.Text,
        time: Date.now() + 1000,
        actor: {
            type: DataHubAiConversationActorType.Agent,
        },
        content: {
            text: 'Agent response',
        },
    };

    beforeEach(() => {
        vi.clearAllMocks();

        // Default mock for useChatStream
        mockUseChatStream.mockReturnValue({
            sendMessage: mockSendMessage,
            stopStreaming: mockStopStreaming,
            isStreaming: false,
            currentMessage: null,
            error: null,
        });

        // Default mock for groupMessages
        mockGroupMessages.mockReturnValue([]);

        // Default mock for createUserMessage
        mockCreateUserMessage.mockReturnValue(mockUserMessage);

        // Mock scrollIntoView
        Element.prototype.scrollIntoView = vi.fn();
    });

    describe('initialization', () => {
        it('should initialize with default state', () => {
            const { result } = renderHook(() =>
                useChatMessages({
                    conversationUrn: mockConversationUrn,
                    userUrn: mockUserUrn,
                }),
            );

            expect(result.current.messages).toEqual([]);
            expect(result.current.messageGroups).toEqual([]);
            expect(result.current.isStreaming).toBe(false);
            expect(result.current.currentMessage).toBeNull();
            expect(typeof result.current.handleSendMessage).toBe('function');
            expect(typeof result.current.handleStopStreaming).toBe('function');
            expect(result.current.messagesEndRef.current).toBeNull();
        });

        it('should initialize useChatStream with correct props', () => {
            const agentName = 'TestAgent';

            renderHook(() =>
                useChatMessages({
                    conversationUrn: mockConversationUrn,
                    userUrn: mockUserUrn,
                    onStreamComplete: mockOnStreamComplete,
                    onMessageReceived: mockOnMessageReceived,
                    agentName,
                }),
            );

            expect(mockUseChatStream).toHaveBeenCalledWith({
                conversationUrn: mockConversationUrn,
                onMessageReceived: expect.any(Function),
                onStreamComplete: mockOnStreamComplete,
                agentName,
            });
        });
    });

    describe('handleSendMessage', () => {
        it('should send a message successfully', () => {
            const { result } = renderHook(() =>
                useChatMessages({
                    conversationUrn: mockConversationUrn,
                    userUrn: mockUserUrn,
                }),
            );

            act(() => {
                result.current.handleSendMessage('Test message');
            });

            expect(mockCreateUserMessage).toHaveBeenCalledWith('Test message', mockUserUrn);
            expect(result.current.messages).toHaveLength(1);
            expect(result.current.messages[0]).toEqual(mockUserMessage);
            expect(mockEmitMessageAnalytics).toHaveBeenCalledWith(mockConversationUrn, 'Test message', 0, 0);
            expect(mockSendMessage).toHaveBeenCalledWith('Test message', undefined);
        });

        it('should not send empty or whitespace-only messages', () => {
            const { result } = renderHook(() =>
                useChatMessages({
                    conversationUrn: mockConversationUrn,
                    userUrn: mockUserUrn,
                }),
            );

            act(() => {
                result.current.handleSendMessage('   ');
            });

            expect(mockCreateUserMessage).not.toHaveBeenCalled();
            expect(result.current.messages).toHaveLength(0);
            expect(mockSendMessage).not.toHaveBeenCalled();
        });

        it('should calculate correct user message index', () => {
            const { result } = renderHook(() =>
                useChatMessages({
                    conversationUrn: mockConversationUrn,
                    userUrn: mockUserUrn,
                }),
            );

            // Set initial messages
            act(() => {
                result.current.setMessages([mockUserMessage, mockAgentMessage]);
            });

            // Send another message
            act(() => {
                result.current.handleSendMessage('Second message');
            });

            // Should count 1 existing user message, so index is 1
            expect(mockEmitMessageAnalytics).toHaveBeenCalledWith(mockConversationUrn, 'Second message', 1, 2);
        });

        it('handleSendMessage should do full flow with optimistic update', () => {
            const { result } = renderHook(() =>
                useChatMessages({
                    conversationUrn: mockConversationUrn,
                    userUrn: mockUserUrn,
                }),
            );

            act(() => {
                result.current.handleSendMessage('Test message');
            });

            // Should add message optimistically
            expect(result.current.messages).toHaveLength(1);

            // Should emit analytics
            expect(mockEmitMessageAnalytics).toHaveBeenCalled();
        });
    });

    describe('handleStopStreaming', () => {
        it('should stop streaming and emit analytics', () => {
            const { result } = renderHook(() =>
                useChatMessages({
                    conversationUrn: mockConversationUrn,
                    userUrn: mockUserUrn,
                }),
            );

            act(() => {
                result.current.handleStopStreaming();
            });

            expect(analytics.event).toHaveBeenCalledWith({
                type: 'StopDataHubChatResponseEvent',
                conversationUrn: mockConversationUrn,
            });
            expect(mockStopStreaming).toHaveBeenCalled();
        });
    });

    describe('message grouping', () => {
        it('should group messages without streaming', () => {
            const { result } = renderHook(() =>
                useChatMessages({
                    conversationUrn: mockConversationUrn,
                    userUrn: mockUserUrn,
                }),
            );

            act(() => {
                result.current.setMessages([mockUserMessage, mockAgentMessage]);
            });

            expect(mockGroupMessages).toHaveBeenCalledWith([mockUserMessage, mockAgentMessage]);
        });

        it('should include current message in grouping when streaming', () => {
            const streamingMessage: DataHubAiConversationMessage = {
                type: DataHubAiConversationMessageType.Text,
                time: Date.now(),
                actor: {
                    type: DataHubAiConversationActorType.Agent,
                },
                content: {
                    text: 'Streaming...',
                },
            };

            mockUseChatStream.mockReturnValue({
                sendMessage: mockSendMessage,
                stopStreaming: mockStopStreaming,
                isStreaming: true,
                currentMessage: streamingMessage,
                error: null,
            });

            const { result } = renderHook(() =>
                useChatMessages({
                    conversationUrn: mockConversationUrn,
                    userUrn: mockUserUrn,
                }),
            );

            act(() => {
                result.current.setMessages([mockUserMessage]);
            });

            expect(mockGroupMessages).toHaveBeenCalledWith([mockUserMessage, streamingMessage]);
        });
    });

    describe('scroll behavior', () => {
        beforeEach(() => {
            vi.useFakeTimers();
        });

        afterEach(() => {
            vi.useRealTimers();
        });

        it('should scroll immediately on initial load with auto behavior', () => {
            const { result } = renderHook(() =>
                useChatMessages({
                    conversationUrn: mockConversationUrn,
                    userUrn: mockUserUrn,
                }),
            );

            const mockDiv = document.createElement('div');
            mockDiv.scrollIntoView = vi.fn();

            act(() => {
                (result.current.messagesEndRef as any).current = mockDiv;
                result.current.setMessages([mockUserMessage]);
            });

            // Fast-forward the timeout
            act(() => {
                vi.advanceTimersByTime(100);
            });

            expect(mockDiv.scrollIntoView).toHaveBeenCalledWith({ behavior: 'auto' });
        });

        it('should scroll smoothly on subsequent message updates', () => {
            const { result } = renderHook(() =>
                useChatMessages({
                    conversationUrn: mockConversationUrn,
                    userUrn: mockUserUrn,
                }),
            );

            const mockDiv = document.createElement('div');
            mockDiv.scrollIntoView = vi.fn();

            act(() => {
                (result.current.messagesEndRef as any).current = mockDiv;
                result.current.setMessages([mockUserMessage]);
            });

            // First scroll (auto)
            act(() => {
                vi.advanceTimersByTime(100);
            });

            expect(mockDiv.scrollIntoView).toHaveBeenCalledWith({ behavior: 'auto' });
            (mockDiv.scrollIntoView as any).mockClear();

            // Add another message
            act(() => {
                result.current.setMessages([mockUserMessage, mockAgentMessage]);
            });

            // Should scroll smoothly now
            expect(mockDiv.scrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' });
        });

        it('should scroll smoothly during streaming', () => {
            const streamingMessage: DataHubAiConversationMessage = {
                type: DataHubAiConversationMessageType.Text,
                time: Date.now(),
                actor: {
                    type: DataHubAiConversationActorType.Agent,
                },
                content: {
                    text: 'Streaming...',
                },
            };

            const { result, rerender } = renderHook(() =>
                useChatMessages({
                    conversationUrn: mockConversationUrn,
                    userUrn: mockUserUrn,
                }),
            );

            const mockDiv = document.createElement('div');
            mockDiv.scrollIntoView = vi.fn();

            act(() => {
                (result.current.messagesEndRef as any).current = mockDiv;
                result.current.setMessages([mockUserMessage]);
            });

            // Initial scroll
            act(() => {
                vi.advanceTimersByTime(100);
            });
            (mockDiv.scrollIntoView as any).mockClear();

            // Start streaming
            mockUseChatStream.mockReturnValue({
                sendMessage: mockSendMessage,
                stopStreaming: mockStopStreaming,
                isStreaming: true,
                currentMessage: streamingMessage,
                error: null,
            });

            rerender();

            expect(mockDiv.scrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' });
        });

        it('should reset scroll flag when conversation changes', () => {
            const { result, rerender } = renderHook(
                ({ conversationUrn }) =>
                    useChatMessages({
                        conversationUrn,
                        userUrn: mockUserUrn,
                    }),
                {
                    initialProps: {
                        conversationUrn: mockConversationUrn,
                    },
                },
            );

            const mockDiv = document.createElement('div');
            mockDiv.scrollIntoView = vi.fn();

            act(() => {
                (result.current.messagesEndRef as any).current = mockDiv;
                result.current.setMessages([mockUserMessage]);
            });

            // Initial scroll (auto)
            act(() => {
                vi.advanceTimersByTime(100);
            });
            expect(mockDiv.scrollIntoView).toHaveBeenCalledWith({ behavior: 'auto' });
            (mockDiv.scrollIntoView as any).mockClear();

            // Change conversation
            rerender({ conversationUrn: 'urn:li:agentConversation:new-conversation' });

            // Add message to new conversation
            act(() => {
                result.current.setMessages([mockAgentMessage]);
            });

            // Should scroll with auto again (flag was reset)
            act(() => {
                vi.advanceTimersByTime(100);
            });
            expect(mockDiv.scrollIntoView).toHaveBeenCalledWith({ behavior: 'auto' });
        });
    });

    describe('cleanup', () => {
        it('should stop streaming on unmount', () => {
            const { unmount } = renderHook(() =>
                useChatMessages({
                    conversationUrn: mockConversationUrn,
                    userUrn: mockUserUrn,
                }),
            );

            unmount();

            expect(mockStopStreaming).toHaveBeenCalled();
        });

        it('should stop streaming when conversation changes', () => {
            const { rerender } = renderHook(
                ({ conversationUrn }) =>
                    useChatMessages({
                        conversationUrn,
                        userUrn: mockUserUrn,
                    }),
                {
                    initialProps: {
                        conversationUrn: mockConversationUrn,
                    },
                },
            );

            rerender({ conversationUrn: 'urn:li:agentConversation:new-conversation' });

            expect(mockStopStreaming).toHaveBeenCalled();
        });
    });

    describe('message receiving', () => {
        it('should skip user messages from stream', () => {
            let capturedOnMessageReceived: any;

            mockUseChatStream.mockImplementation((props) => {
                capturedOnMessageReceived = props.onMessageReceived;
                return {
                    sendMessage: mockSendMessage,
                    stopStreaming: mockStopStreaming,
                    isStreaming: false,
                    currentMessage: null,
                    error: null,
                };
            });

            const { result } = renderHook(() =>
                useChatMessages({
                    conversationUrn: mockConversationUrn,
                    userUrn: mockUserUrn,
                }),
            );

            // Simulate receiving a user message from stream
            act(() => {
                capturedOnMessageReceived(mockUserMessage);
            });

            // Should not be added (we add user messages optimistically)
            expect(result.current.messages).toHaveLength(0);
        });

        it('should add agent messages from stream', () => {
            let capturedOnMessageReceived: any;

            mockUseChatStream.mockImplementation((props) => {
                capturedOnMessageReceived = props.onMessageReceived;
                return {
                    sendMessage: mockSendMessage,
                    stopStreaming: mockStopStreaming,
                    isStreaming: false,
                    currentMessage: null,
                    error: null,
                };
            });

            const { result } = renderHook(() =>
                useChatMessages({
                    conversationUrn: mockConversationUrn,
                    userUrn: mockUserUrn,
                }),
            );

            // Simulate receiving an agent message from stream
            act(() => {
                capturedOnMessageReceived(mockAgentMessage);
            });

            expect(result.current.messages).toHaveLength(1);
            expect(result.current.messages[0]).toEqual(mockAgentMessage);
        });

        it('should not add duplicate messages', () => {
            let capturedOnMessageReceived: any;

            mockUseChatStream.mockImplementation((props) => {
                capturedOnMessageReceived = props.onMessageReceived;
                return {
                    sendMessage: mockSendMessage,
                    stopStreaming: mockStopStreaming,
                    isStreaming: false,
                    currentMessage: null,
                    error: null,
                };
            });

            const { result } = renderHook(() =>
                useChatMessages({
                    conversationUrn: mockConversationUrn,
                    userUrn: mockUserUrn,
                }),
            );

            // Add same message twice
            act(() => {
                capturedOnMessageReceived(mockAgentMessage);
                capturedOnMessageReceived(mockAgentMessage);
            });

            // Should only be added once
            expect(result.current.messages).toHaveLength(1);
        });

        it('should call custom onMessageReceived callback', () => {
            let capturedOnMessageReceived: any;

            mockUseChatStream.mockImplementation((props) => {
                capturedOnMessageReceived = props.onMessageReceived;
                return {
                    sendMessage: mockSendMessage,
                    stopStreaming: mockStopStreaming,
                    isStreaming: false,
                    currentMessage: null,
                    error: null,
                };
            });

            const customCallback = vi.fn();

            renderHook(() =>
                useChatMessages({
                    conversationUrn: mockConversationUrn,
                    userUrn: mockUserUrn,
                    onMessageReceived: customCallback,
                }),
            );

            act(() => {
                capturedOnMessageReceived(mockAgentMessage);
            });

            expect(customCallback).toHaveBeenCalledWith(mockAgentMessage);
        });
    });
});
