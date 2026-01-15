import { act, renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ChatLocationType } from '@app/analytics';
import { useChatMessageReaction } from '@app/chat/hooks/useChatMessageReaction';
import { emitFeedbackEvent, emitReactionEvent } from '@app/chat/utils/chatFeedbackUtils';

// Mock the utility functions
vi.mock('@app/chat/utils/chatFeedbackUtils', () => ({
    generateMessageId: vi.fn((time, text) => `${time}-${(text || '').substring(0, 20)}`),
    emitReactionEvent: vi.fn(() => true),
    emitFeedbackEvent: vi.fn(() => true),
}));

describe('useChatMessageReaction', () => {
    const defaultProps = {
        messageTime: 1234567890,
        messageText: 'Test message content',
        conversationUrn: 'urn:li:dataHubAiConversation:123',
        chatLocation: 'ask_datahub_ui' as ChatLocationType,
    };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should initialize with null reaction and modal hidden', () => {
        const { result } = renderHook(() => useChatMessageReaction(defaultProps));

        expect(result.current.reaction).toBeNull();
        expect(result.current.showFeedbackModal).toBe(false);
    });

    describe('handleReaction', () => {
        it('should set positive reaction and emit event', () => {
            const { result } = renderHook(() => useChatMessageReaction(defaultProps));

            act(() => {
                result.current.handleReaction('positive');
            });

            expect(result.current.reaction).toBe('positive');
            expect(emitReactionEvent).toHaveBeenCalledWith({
                conversationUrn: defaultProps.conversationUrn,
                messageId: '1234567890-Test message content',
                reaction: 'positive',
                chatLocation: defaultProps.chatLocation,
            });
            expect(result.current.showFeedbackModal).toBe(false);
        });

        it('should set negative reaction, emit event, and show feedback modal', () => {
            const { result } = renderHook(() => useChatMessageReaction(defaultProps));

            act(() => {
                result.current.handleReaction('negative');
            });

            expect(result.current.reaction).toBe('negative');
            expect(emitReactionEvent).toHaveBeenCalledWith({
                conversationUrn: defaultProps.conversationUrn,
                messageId: '1234567890-Test message content',
                reaction: 'negative',
                chatLocation: defaultProps.chatLocation,
            });
            expect(result.current.showFeedbackModal).toBe(true);
        });

        it('should allow changing reaction', () => {
            const { result } = renderHook(() => useChatMessageReaction(defaultProps));

            act(() => {
                result.current.handleReaction('positive');
            });
            expect(result.current.reaction).toBe('positive');

            act(() => {
                result.current.handleReaction('negative');
            });
            expect(result.current.reaction).toBe('negative');
            expect(emitReactionEvent).toHaveBeenCalledTimes(2);
        });

        it('should not emit event if conversationUrn is missing', () => {
            const { result } = renderHook(() =>
                useChatMessageReaction({
                    ...defaultProps,
                    conversationUrn: undefined,
                }),
            );

            act(() => {
                result.current.handleReaction('positive');
            });

            expect(result.current.reaction).toBe('positive');
            expect(emitReactionEvent).not.toHaveBeenCalled();
        });

        it('should not emit event if chatLocation is missing', () => {
            const { result } = renderHook(() =>
                useChatMessageReaction({
                    ...defaultProps,
                    chatLocation: undefined,
                }),
            );

            act(() => {
                result.current.handleReaction('positive');
            });

            expect(result.current.reaction).toBe('positive');
            expect(emitReactionEvent).not.toHaveBeenCalled();
        });
    });

    describe('handleFeedbackSubmit', () => {
        it('should emit feedback event and close modal', () => {
            const { result } = renderHook(() => useChatMessageReaction(defaultProps));

            // First trigger negative reaction to open modal
            act(() => {
                result.current.handleReaction('negative');
            });
            expect(result.current.showFeedbackModal).toBe(true);

            // Submit feedback
            act(() => {
                result.current.handleFeedbackSubmit('The response was not helpful');
            });

            expect(emitFeedbackEvent).toHaveBeenCalledWith({
                conversationUrn: defaultProps.conversationUrn,
                messageId: '1234567890-Test message content',
                feedbackText: 'The response was not helpful',
                chatLocation: defaultProps.chatLocation,
            });
            expect(result.current.showFeedbackModal).toBe(false);
        });

        it('should not emit event if conversationUrn is missing', () => {
            const { result } = renderHook(() =>
                useChatMessageReaction({
                    ...defaultProps,
                    conversationUrn: undefined,
                }),
            );

            act(() => {
                result.current.handleReaction('negative');
            });

            act(() => {
                result.current.handleFeedbackSubmit('Feedback text');
            });

            expect(emitFeedbackEvent).not.toHaveBeenCalled();
            expect(result.current.showFeedbackModal).toBe(false);
        });
    });

    describe('handleFeedbackCancel', () => {
        it('should close modal without emitting event', () => {
            const { result } = renderHook(() => useChatMessageReaction(defaultProps));

            // First trigger negative reaction to open modal
            act(() => {
                result.current.handleReaction('negative');
            });
            expect(result.current.showFeedbackModal).toBe(true);

            // Cancel
            act(() => {
                result.current.handleFeedbackCancel();
            });

            expect(result.current.showFeedbackModal).toBe(false);
            expect(emitFeedbackEvent).not.toHaveBeenCalled();
        });
    });

    describe('with different chat locations', () => {
        const locations: ChatLocationType[] = [
            'ask_datahub_ui',
            'ask_datahub_tab',
            'ingestion_configure_source',
            'ingestion_view_results',
        ];

        it.each(locations)('should work correctly with chatLocation: %s', (location) => {
            const { result } = renderHook(() =>
                useChatMessageReaction({
                    ...defaultProps,
                    chatLocation: location,
                }),
            );

            act(() => {
                result.current.handleReaction('positive');
            });

            expect(emitReactionEvent).toHaveBeenCalledWith(
                expect.objectContaining({
                    chatLocation: location,
                }),
            );
        });
    });
});
