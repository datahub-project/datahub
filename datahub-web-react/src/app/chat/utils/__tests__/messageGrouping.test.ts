import { describe, expect, it } from 'vitest';

import {
    extractTextMessages,
    groupMessages,
    isThinkingOrToolMessage,
    shouldAddMessage,
} from '@app/chat/utils/messageGrouping';

import { DataHubAiConversationActorType, DataHubAiConversationMessage, DataHubAiConversationMessageType } from '@types';

describe('messageGrouping', () => {
    describe('isThinkingOrToolMessage', () => {
        it('returns true for thinking messages', () => {
            const message: DataHubAiConversationMessage = {
                type: DataHubAiConversationMessageType.Thinking,
                time: Date.now(),
                actor: { type: DataHubAiConversationActorType.Agent },
                content: { text: 'Thinking...' },
            };

            expect(isThinkingOrToolMessage(message)).toBe(true);
        });

        it('returns true for tool call messages', () => {
            const message: DataHubAiConversationMessage = {
                type: DataHubAiConversationMessageType.ToolCall,
                time: Date.now(),
                actor: { type: DataHubAiConversationActorType.Agent },
                content: { text: 'Tool call' },
            };

            expect(isThinkingOrToolMessage(message)).toBe(true);
        });

        it('returns true for tool result messages', () => {
            const message: DataHubAiConversationMessage = {
                type: DataHubAiConversationMessageType.ToolResult,
                time: Date.now(),
                actor: { type: DataHubAiConversationActorType.Agent },
                content: { text: 'Tool result' },
            };

            expect(isThinkingOrToolMessage(message)).toBe(true);
        });

        it('returns false for text messages', () => {
            const message: DataHubAiConversationMessage = {
                type: DataHubAiConversationMessageType.Text,
                time: Date.now(),
                actor: { type: DataHubAiConversationActorType.User, actor: 'urn:li:corpuser:test' },
                content: { text: 'Hello world' },
            };

            expect(isThinkingOrToolMessage(message)).toBe(false);
        });
    });

    describe('groupMessages', () => {
        it('returns empty array for empty input', () => {
            expect(groupMessages([])).toEqual([]);
        });

        it('groups single text message as regular', () => {
            const messages: DataHubAiConversationMessage[] = [
                {
                    type: DataHubAiConversationMessageType.Text,
                    time: 1,
                    actor: { type: DataHubAiConversationActorType.User, actor: 'urn:li:corpuser:test' },
                    content: { text: 'Hello' },
                },
            ];

            const result = groupMessages(messages);

            expect(result).toEqual([{ type: 'regular', message: messages[0] }]);
        });

        it('groups consecutive thinking messages together', () => {
            const messages: DataHubAiConversationMessage[] = [
                {
                    type: DataHubAiConversationMessageType.Thinking,
                    time: 1,
                    actor: { type: DataHubAiConversationActorType.Agent },
                    content: { text: 'Thinking...' },
                },
                {
                    type: DataHubAiConversationMessageType.ToolCall,
                    time: 2,
                    actor: { type: DataHubAiConversationActorType.Agent },
                    content: { text: 'Calling tool' },
                },
                {
                    type: DataHubAiConversationMessageType.ToolResult,
                    time: 3,
                    actor: { type: DataHubAiConversationActorType.Agent },
                    content: { text: 'Tool result' },
                },
            ];

            const result = groupMessages(messages);

            expect(result).toEqual([{ type: 'thinking', messages }]);
        });

        it('separates thinking groups with regular messages', () => {
            const messages: DataHubAiConversationMessage[] = [
                {
                    type: DataHubAiConversationMessageType.Text,
                    time: 1,
                    actor: { type: DataHubAiConversationActorType.User, actor: 'urn:li:corpuser:test' },
                    content: { text: 'User message' },
                },
                {
                    type: DataHubAiConversationMessageType.Thinking,
                    time: 2,
                    actor: { type: DataHubAiConversationActorType.Agent },
                    content: { text: 'Thinking...' },
                },
                {
                    type: DataHubAiConversationMessageType.ToolCall,
                    time: 3,
                    actor: { type: DataHubAiConversationActorType.Agent },
                    content: { text: 'Tool call' },
                },
                {
                    type: DataHubAiConversationMessageType.Text,
                    time: 4,
                    actor: { type: DataHubAiConversationActorType.Agent },
                    content: { text: 'Agent response' },
                },
            ];

            const result = groupMessages(messages);

            expect(result).toHaveLength(3);
            expect(result[0]).toEqual({ type: 'regular', message: messages[0] });
            expect(result[1]).toEqual({ type: 'thinking', messages: [messages[1], messages[2]] });
            expect(result[2]).toEqual({ type: 'regular', message: messages[3] });
        });

        it('handles multiple thinking groups separated by regular messages', () => {
            const messages: DataHubAiConversationMessage[] = [
                {
                    type: DataHubAiConversationMessageType.Thinking,
                    time: 1,
                    actor: { type: DataHubAiConversationActorType.Agent },
                    content: { text: 'First thinking' },
                },
                {
                    type: DataHubAiConversationMessageType.Text,
                    time: 2,
                    actor: { type: DataHubAiConversationActorType.Agent },
                    content: { text: 'Response 1' },
                },
                {
                    type: DataHubAiConversationMessageType.Thinking,
                    time: 3,
                    actor: { type: DataHubAiConversationActorType.Agent },
                    content: { text: 'Second thinking' },
                },
                {
                    type: DataHubAiConversationMessageType.Text,
                    time: 4,
                    actor: { type: DataHubAiConversationActorType.Agent },
                    content: { text: 'Response 2' },
                },
            ];

            const result = groupMessages(messages);

            expect(result).toHaveLength(4);
            expect(result[0]).toEqual({ type: 'thinking', messages: [messages[0]] });
            expect(result[1]).toEqual({ type: 'regular', message: messages[1] });
            expect(result[2]).toEqual({ type: 'thinking', messages: [messages[2]] });
            expect(result[3]).toEqual({ type: 'regular', message: messages[3] });
        });

        it('handles thinking messages at the end', () => {
            const messages: DataHubAiConversationMessage[] = [
                {
                    type: DataHubAiConversationMessageType.Text,
                    time: 1,
                    actor: { type: DataHubAiConversationActorType.User, actor: 'urn:li:corpuser:test' },
                    content: { text: 'User message' },
                },
                {
                    type: DataHubAiConversationMessageType.Thinking,
                    time: 2,
                    actor: { type: DataHubAiConversationActorType.Agent },
                    content: { text: 'Still thinking...' },
                },
            ];

            const result = groupMessages(messages);

            expect(result).toHaveLength(2);
            expect(result[0]).toEqual({ type: 'regular', message: messages[0] });
            expect(result[1]).toEqual({ type: 'thinking', messages: [messages[1]] });
        });
    });

    describe('extractTextMessages', () => {
        it('filters out thinking and tool messages', () => {
            const messages: DataHubAiConversationMessage[] = [
                {
                    type: DataHubAiConversationMessageType.Text,
                    content: { text: 'Question' },
                    actor: { type: DataHubAiConversationActorType.User },
                    time: 1000,
                },
                {
                    type: DataHubAiConversationMessageType.Thinking,
                    content: { text: 'Thinking...' },
                    actor: { type: DataHubAiConversationActorType.Agent },
                    time: 2000,
                },
                {
                    type: DataHubAiConversationMessageType.Text,
                    content: { text: 'Answer' },
                    actor: { type: DataHubAiConversationActorType.Agent },
                    time: 3000,
                },
            ];

            const filtered = extractTextMessages(messages);

            expect(filtered).toHaveLength(2);
            expect(filtered[0].time).toBe(1000);
            expect(filtered[1].time).toBe(3000);
        });

        it('returns empty array when only thinking messages', () => {
            const messages: DataHubAiConversationMessage[] = [
                {
                    type: DataHubAiConversationMessageType.Thinking,
                    content: { text: 'Thinking...' },
                    actor: { type: DataHubAiConversationActorType.Agent },
                    time: 1000,
                },
            ];

            const filtered = extractTextMessages(messages);

            expect(filtered).toHaveLength(0);
        });
    });

    describe('shouldAddMessage', () => {
        it('returns true for new messages', () => {
            const existingMessages: DataHubAiConversationMessage[] = [
                {
                    type: DataHubAiConversationMessageType.Text,
                    content: { text: 'Hello' },
                    actor: { type: DataHubAiConversationActorType.User },
                    time: 1000,
                },
            ];

            const newMessage: DataHubAiConversationMessage = {
                type: DataHubAiConversationMessageType.Text,
                content: { text: 'Response' },
                actor: { type: DataHubAiConversationActorType.Agent },
                time: 2000,
            };

            expect(shouldAddMessage(existingMessages, newMessage)).toBe(true);
        });

        it('returns false for duplicate messages with same time and content', () => {
            const existingMessages: DataHubAiConversationMessage[] = [
                {
                    type: DataHubAiConversationMessageType.Text,
                    content: { text: 'Hello' },
                    actor: { type: DataHubAiConversationActorType.User },
                    time: 1000,
                },
            ];

            const duplicateMessage: DataHubAiConversationMessage = {
                type: DataHubAiConversationMessageType.Text,
                content: { text: 'Hello' },
                actor: { type: DataHubAiConversationActorType.User },
                time: 1000,
            };

            expect(shouldAddMessage(existingMessages, duplicateMessage)).toBe(false);
        });

        it('returns true for messages with same content but different time', () => {
            const existingMessages: DataHubAiConversationMessage[] = [
                {
                    type: DataHubAiConversationMessageType.Text,
                    content: { text: 'Hello' },
                    actor: { type: DataHubAiConversationActorType.User },
                    time: 1000,
                },
            ];

            const newMessage: DataHubAiConversationMessage = {
                type: DataHubAiConversationMessageType.Text,
                content: { text: 'Hello' },
                actor: { type: DataHubAiConversationActorType.User },
                time: 2000,
            };

            expect(shouldAddMessage(existingMessages, newMessage)).toBe(true);
        });
    });
});
