import { THINKING_WINDOW_MS, isUserOrAgentThinkingRecent, shouldPollForThinking } from '@app/chat/utils/thinkingState';

import { DataHubAiConversationActorType, DataHubAiConversationMessage, DataHubAiConversationMessageType } from '@types';

const NOW = 1_000_000;

function userMessage(time: number): DataHubAiConversationMessage {
    return {
        actor: { type: DataHubAiConversationActorType.User },
        content: { text: 'user' },
        time,
        type: DataHubAiConversationMessageType.Text,
    };
}

function agentThinking(time: number): DataHubAiConversationMessage {
    return {
        actor: { type: DataHubAiConversationActorType.Agent },
        content: { text: '' },
        time,
        type: DataHubAiConversationMessageType.Thinking,
    };
}

function agentText(time: number): DataHubAiConversationMessage {
    return {
        actor: { type: DataHubAiConversationActorType.Agent },
        content: { text: 'hi' },
        time,
        type: DataHubAiConversationMessageType.Text,
    };
}

describe('thinkingPredicate', () => {
    it('returns true for recent user message', () => {
        expect(isUserOrAgentThinkingRecent([userMessage(NOW - 1_000)], NOW)).toBe(true);
    });

    it('returns true for recent agent thinking message', () => {
        expect(isUserOrAgentThinkingRecent([agentThinking(NOW - 1_000)], NOW)).toBe(true);
    });

    it('returns false when no messages', () => {
        expect(isUserOrAgentThinkingRecent([], NOW)).toBe(false);
    });

    it('returns false when last message is agent text', () => {
        expect(isUserOrAgentThinkingRecent([agentText(NOW - 1_000)], NOW)).toBe(false);
    });

    it('returns false when outside the thinking window', () => {
        expect(isUserOrAgentThinkingRecent([userMessage(NOW - THINKING_WINDOW_MS - 1)], NOW)).toBe(false);
    });

    describe('shouldPollForThinking', () => {
        it('returns false when streaming', () => {
            expect(
                shouldPollForThinking({
                    messages: [userMessage(NOW - 1_000)],
                    isStreaming: true,
                    now: NOW,
                }),
            ).toBe(false);
        });

        it('returns false when no messages', () => {
            expect(
                shouldPollForThinking({
                    messages: [],
                    isStreaming: false,
                    now: NOW,
                }),
            ).toBe(false);
        });

        it('returns true for recent user message when not streaming', () => {
            expect(
                shouldPollForThinking({
                    messages: [userMessage(NOW - 1_000)],
                    isStreaming: false,
                    now: NOW,
                }),
            ).toBe(true);
        });

        it('returns true for recent agent thinking when not streaming', () => {
            expect(
                shouldPollForThinking({
                    messages: [agentThinking(NOW - 1_000)],
                    isStreaming: false,
                    now: NOW,
                }),
            ).toBe(true);
        });

        it('returns false for agent text', () => {
            expect(
                shouldPollForThinking({
                    messages: [agentText(NOW - 1_000)],
                    isStreaming: false,
                    now: NOW,
                }),
            ).toBe(false);
        });

        it('returns false when outside the window', () => {
            expect(
                shouldPollForThinking({
                    messages: [userMessage(NOW - THINKING_WINDOW_MS - 1)],
                    isStreaming: false,
                    now: NOW,
                }),
            ).toBe(false);
        });
    });
});
