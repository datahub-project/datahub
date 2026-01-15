import { beforeEach, describe, expect, it, vi } from 'vitest';

import analytics, { ChatLocationType, EventType } from '@app/analytics';
import { emitFeedbackEvent, emitReactionEvent, generateMessageId } from '@app/chat/utils/chatFeedbackUtils';

// Mock analytics
vi.mock('@app/analytics', () => ({
    default: {
        event: vi.fn(),
    },
    EventType: {
        ChatMessageReactionEvent: 'ChatMessageReactionEvent',
        ChatMessageFeedbackEvent: 'ChatMessageFeedbackEvent',
    },
}));

describe('chatFeedbackUtils', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('generateMessageId', () => {
        it('should generate ID from timestamp and text preview', () => {
            const result = generateMessageId(1234567890, 'Hello world this is a test message');
            expect(result).toBe('1234567890-Hello world this is ');
        });

        it('should handle empty text', () => {
            const result = generateMessageId(1234567890, '');
            expect(result).toBe('1234567890-');
        });

        it('should handle undefined text', () => {
            const result = generateMessageId(1234567890, undefined);
            expect(result).toBe('1234567890-');
        });

        it('should handle short text', () => {
            const result = generateMessageId(1234567890, 'Hi');
            expect(result).toBe('1234567890-Hi');
        });

        it('should truncate text at 20 characters', () => {
            const result = generateMessageId(1234567890, '12345678901234567890extra');
            expect(result).toBe('1234567890-12345678901234567890');
        });
    });

    describe('emitReactionEvent', () => {
        const validParams = {
            conversationUrn: 'urn:li:dataHubAiConversation:123',
            messageId: '1234567890-Hello',
            reaction: 'positive' as const,
            chatLocation: 'ask_datahub_ui' as ChatLocationType,
        };

        it('should emit analytics event with correct params', () => {
            const result = emitReactionEvent(validParams);

            expect(result).toBe(true);
            expect(analytics.event).toHaveBeenCalledWith({
                type: EventType.ChatMessageReactionEvent,
                conversationUrn: validParams.conversationUrn,
                messageId: validParams.messageId,
                reaction: validParams.reaction,
                chatLocation: validParams.chatLocation,
            });
        });

        it('should return false if conversationUrn is missing', () => {
            const result = emitReactionEvent({
                ...validParams,
                conversationUrn: '',
            });

            expect(result).toBe(false);
            expect(analytics.event).not.toHaveBeenCalled();
        });

        it('should return false if chatLocation is missing', () => {
            const result = emitReactionEvent({
                ...validParams,
                chatLocation: '' as ChatLocationType,
            });

            expect(result).toBe(false);
            expect(analytics.event).not.toHaveBeenCalled();
        });

        it('should handle negative reaction', () => {
            const result = emitReactionEvent({
                ...validParams,
                reaction: 'negative',
            });

            expect(result).toBe(true);
            expect(analytics.event).toHaveBeenCalledWith(
                expect.objectContaining({
                    reaction: 'negative',
                }),
            );
        });

        it('should work with all chat location types', () => {
            const locations: ChatLocationType[] = [
                'ask_datahub_ui',
                'ask_datahub_tab',
                'ingestion_configure_source',
                'ingestion_view_results',
            ];

            locations.forEach((location) => {
                vi.clearAllMocks();
                const result = emitReactionEvent({
                    ...validParams,
                    chatLocation: location,
                });

                expect(result).toBe(true);
                expect(analytics.event).toHaveBeenCalledWith(
                    expect.objectContaining({
                        chatLocation: location,
                    }),
                );
            });
        });
    });

    describe('emitFeedbackEvent', () => {
        const validParams = {
            conversationUrn: 'urn:li:dataHubAiConversation:123',
            messageId: '1234567890-Hello',
            feedbackText: 'The response was not accurate',
            chatLocation: 'ask_datahub_ui' as ChatLocationType,
        };

        it('should emit analytics event with correct params', () => {
            const result = emitFeedbackEvent(validParams);

            expect(result).toBe(true);
            expect(analytics.event).toHaveBeenCalledWith({
                type: EventType.ChatMessageFeedbackEvent,
                conversationUrn: validParams.conversationUrn,
                messageId: validParams.messageId,
                feedbackText: validParams.feedbackText,
                chatLocation: validParams.chatLocation,
            });
        });

        it('should return false if conversationUrn is missing', () => {
            const result = emitFeedbackEvent({
                ...validParams,
                conversationUrn: '',
            });

            expect(result).toBe(false);
            expect(analytics.event).not.toHaveBeenCalled();
        });

        it('should return false if chatLocation is missing', () => {
            const result = emitFeedbackEvent({
                ...validParams,
                chatLocation: '' as ChatLocationType,
            });

            expect(result).toBe(false);
            expect(analytics.event).not.toHaveBeenCalled();
        });

        it('should handle empty feedback text', () => {
            const result = emitFeedbackEvent({
                ...validParams,
                feedbackText: '',
            });

            expect(result).toBe(true);
            expect(analytics.event).toHaveBeenCalledWith(
                expect.objectContaining({
                    feedbackText: '',
                }),
            );
        });

        it('should work with all chat location types', () => {
            const locations: ChatLocationType[] = [
                'ask_datahub_ui',
                'ask_datahub_tab',
                'ingestion_configure_source',
                'ingestion_view_results',
            ];

            locations.forEach((location) => {
                vi.clearAllMocks();
                const result = emitFeedbackEvent({
                    ...validParams,
                    chatLocation: location,
                });

                expect(result).toBe(true);
                expect(analytics.event).toHaveBeenCalledWith(
                    expect.objectContaining({
                        chatLocation: location,
                    }),
                );
            });
        });
    });
});
