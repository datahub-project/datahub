import { renderHook } from '@testing-library/react-hooks';
import { vi } from 'vitest';

import { useSyncChatMode } from '@app/chat/hooks/useSyncChatMode';
import { DEFAULT_CHAT_MODE } from '@app/chat/utils/chatModes';

describe('useSyncChatMode', () => {
    const mockOnModeChange = vi.fn();

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('initial sync behavior', () => {
        it('should sync mode when messages are immediately available', () => {
            const messages = [{ agentName: 'AskDataHubResearch' }];

            renderHook(() =>
                useSyncChatMode({
                    conversationUrn: 'urn:conversation:1',
                    messages,
                    onModeChange: mockOnModeChange,
                }),
            );

            expect(mockOnModeChange).toHaveBeenCalledTimes(1);
            expect(mockOnModeChange).toHaveBeenCalledWith('research');
        });

        it('should sync mode when messages become available asynchronously', () => {
            const { rerender } = renderHook(
                ({ messages }) =>
                    useSyncChatMode({
                        conversationUrn: 'urn:conversation:1',
                        messages,
                        onModeChange: mockOnModeChange,
                    }),
                {
                    initialProps: { messages: undefined as Array<{ agentName?: string | null }> | undefined },
                },
            );

            // Initially no messages - should not call onModeChange
            expect(mockOnModeChange).not.toHaveBeenCalled();

            // Messages become available
            rerender({ messages: [{ agentName: 'AskDataHubFast' }] });

            expect(mockOnModeChange).toHaveBeenCalledTimes(1);
            expect(mockOnModeChange).toHaveBeenCalledWith('fast');
        });

        it('should only sync once per conversation', () => {
            const messages = [{ agentName: 'AskDataHubResearch' }];

            const { rerender } = renderHook(
                ({ messages: msgs }) =>
                    useSyncChatMode({
                        conversationUrn: 'urn:conversation:1',
                        messages: msgs,
                        onModeChange: mockOnModeChange,
                    }),
                {
                    initialProps: { messages },
                },
            );

            expect(mockOnModeChange).toHaveBeenCalledTimes(1);

            // Rerender with same conversation - should NOT call again
            rerender({ messages: [...messages, { agentName: 'AskDataHubFast' }] });

            expect(mockOnModeChange).toHaveBeenCalledTimes(1);
        });
    });

    describe('conversation switching', () => {
        it('should sync mode when switching to a new conversation', () => {
            const { rerender } = renderHook(
                ({ conversationUrn, messages }) =>
                    useSyncChatMode({
                        conversationUrn,
                        messages,
                        onModeChange: mockOnModeChange,
                    }),
                {
                    initialProps: {
                        conversationUrn: 'urn:conversation:1',
                        messages: [{ agentName: 'AskDataHubResearch' }],
                    },
                },
            );

            expect(mockOnModeChange).toHaveBeenCalledWith('research');

            // Switch to a new conversation
            rerender({
                conversationUrn: 'urn:conversation:2',
                messages: [{ agentName: 'AskDataHubFast' }],
            });

            expect(mockOnModeChange).toHaveBeenCalledTimes(2);
            expect(mockOnModeChange).toHaveBeenLastCalledWith('fast');
        });

        it('should handle switching to conversation with no messages', () => {
            const { rerender } = renderHook(
                ({ conversationUrn, messages }) =>
                    useSyncChatMode({
                        conversationUrn,
                        messages,
                        onModeChange: mockOnModeChange,
                    }),
                {
                    initialProps: {
                        conversationUrn: 'urn:conversation:1',
                        messages: [{ agentName: 'AskDataHubResearch' }] as
                            | Array<{ agentName?: string | null }>
                            | undefined,
                    },
                },
            );

            expect(mockOnModeChange).toHaveBeenCalledWith('research');

            // Switch to new conversation with no messages yet (loading)
            rerender({
                conversationUrn: 'urn:conversation:2',
                messages: undefined,
            });

            // Should not call again yet
            expect(mockOnModeChange).toHaveBeenCalledTimes(1);

            // Messages load for new conversation
            rerender({
                conversationUrn: 'urn:conversation:2',
                messages: [{ agentName: 'AskDataHubFast' }],
            });

            expect(mockOnModeChange).toHaveBeenCalledTimes(2);
            expect(mockOnModeChange).toHaveBeenLastCalledWith('fast');
        });
    });

    describe('fallback behavior', () => {
        it('should fall back to default mode when no messages have agentName', () => {
            const messages = [{ agentName: null }, { agentName: undefined }, {}];

            renderHook(() =>
                useSyncChatMode({
                    conversationUrn: 'urn:conversation:1',
                    messages,
                    onModeChange: mockOnModeChange,
                }),
            );

            expect(mockOnModeChange).toHaveBeenCalledWith(DEFAULT_CHAT_MODE);
        });

        it('should fall back to default mode for empty messages array', () => {
            renderHook(() =>
                useSyncChatMode({
                    conversationUrn: 'urn:conversation:1',
                    messages: [],
                    onModeChange: mockOnModeChange,
                }),
            );

            expect(mockOnModeChange).toHaveBeenCalledWith(DEFAULT_CHAT_MODE);
        });

        it('should fall back to default mode for unknown agent name', () => {
            const messages = [{ agentName: 'UnknownAgent' }];

            renderHook(() =>
                useSyncChatMode({
                    conversationUrn: 'urn:conversation:1',
                    messages,
                    onModeChange: mockOnModeChange,
                }),
            );

            expect(mockOnModeChange).toHaveBeenCalledWith(DEFAULT_CHAT_MODE);
        });
    });

    describe('mode selection from messages', () => {
        it('should use agentName from most recent message', () => {
            const messages = [
                { agentName: 'AskDataHubAuto' },
                { agentName: 'AskDataHubResearch' },
                { agentName: null }, // Most recent message has no agentName
            ];

            renderHook(() =>
                useSyncChatMode({
                    conversationUrn: 'urn:conversation:1',
                    messages,
                    onModeChange: mockOnModeChange,
                }),
            );

            // Should use 'research' from the second-to-last message (skipping null)
            expect(mockOnModeChange).toHaveBeenCalledWith('research');
        });
    });
});
