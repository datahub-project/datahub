import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import analytics from '@app/analytics';
import { createUserMessage, emitMessageAnalytics } from '@app/chat/utils/chatMessageUtils';

import { DataHubAiConversationActorType, DataHubAiConversationMessageType } from '@types';

// Mock analytics
vi.mock('@app/analytics', () => ({
    default: {
        event: vi.fn(),
    },
    EventType: {
        CreateDataHubChatMessageEvent: 'CreateDataHubChatMessageEvent',
    },
}));

// Mock extractReferencesFromMarkdown
vi.mock('@app/chat/utils/extractUrnsFromMarkdown', () => ({
    extractReferencesFromMarkdown: vi.fn((text: string) => {
        // Simple mock: return mentions if text contains @ symbol
        const matches = text.match(/@\[([^\]]+)\]/g) || [];
        return matches.map((match) => ({
            urn: `urn:li:dataset:${match}`,
            displayName: match,
        }));
    }),
}));

describe('chatMessageUtils', () => {
    describe('createUserMessage', () => {
        beforeEach(() => {
            vi.useFakeTimers();
        });

        afterEach(() => {
            vi.useRealTimers();
        });

        it('creates a user message with correct structure', () => {
            const mockTime = 1234567890;
            vi.setSystemTime(mockTime);

            const message = createUserMessage('Hello world', 'urn:li:corpuser:test');

            expect(message).toEqual({
                type: DataHubAiConversationMessageType.Text,
                time: mockTime,
                actor: {
                    type: DataHubAiConversationActorType.User,
                    actor: 'urn:li:corpuser:test',
                },
                content: {
                    text: 'Hello world',
                },
            });
        });

        it('handles empty text', () => {
            const message = createUserMessage('', 'urn:li:corpuser:test');

            expect(message.content.text).toBe('');
            expect(message.actor.type).toBe(DataHubAiConversationActorType.User);
        });

        it('preserves message text exactly as provided', () => {
            const textWithSpecialChars = 'Test @mention #hashtag\nNew line';
            const message = createUserMessage(textWithSpecialChars, 'urn:li:corpuser:user123');

            expect(message.content.text).toBe(textWithSpecialChars);
            expect(message.actor.actor).toBe('urn:li:corpuser:user123');
        });
    });

    describe('emitMessageAnalytics', () => {
        beforeEach(() => {
            vi.clearAllMocks();
        });

        it('emits analytics event with correct basic properties', () => {
            emitMessageAnalytics('urn:li:conversation:123', 'Hello world', 0, 0);

            expect(analytics.event).toHaveBeenCalledWith({
                type: 'CreateDataHubChatMessageEvent',
                conversationUrn: 'urn:li:conversation:123',
                messageLength: 11,
                hasEntityMentions: false,
                entityMentionCount: 0,
                userMessageIndex: 0,
                totalMessageCount: 0,
                messagePreview: 'Hello world',
            });
        });

        it('detects entity mentions in message text', () => {
            const messageWithMentions = 'Check out @[dataset1] and @[dataset2]';

            emitMessageAnalytics('urn:li:conversation:123', messageWithMentions, 1, 2);

            expect(analytics.event).toHaveBeenCalledWith(
                expect.objectContaining({
                    hasEntityMentions: true,
                    entityMentionCount: 2,
                    userMessageIndex: 1,
                    totalMessageCount: 2,
                }),
            );
        });

        it('truncates long messages to 200 characters in preview', () => {
            const longMessage = 'a'.repeat(300);

            emitMessageAnalytics('urn:li:conversation:123', longMessage, 0, 0);

            expect(analytics.event).toHaveBeenCalledWith(
                expect.objectContaining({
                    messageLength: 300,
                    messagePreview: 'a'.repeat(200),
                }),
            );
        });

        it('handles message without mentions', () => {
            emitMessageAnalytics('urn:li:conversation:456', 'Simple message', 2, 5);

            expect(analytics.event).toHaveBeenCalledWith(
                expect.objectContaining({
                    hasEntityMentions: false,
                    entityMentionCount: 0,
                    conversationUrn: 'urn:li:conversation:456',
                }),
            );
        });

        it('includes correct message indices', () => {
            emitMessageAnalytics('urn:li:conversation:789', 'Message text', 5, 10);

            expect(analytics.event).toHaveBeenCalledWith(
                expect.objectContaining({
                    userMessageIndex: 5,
                    totalMessageCount: 10,
                }),
            );
        });
    });
});
