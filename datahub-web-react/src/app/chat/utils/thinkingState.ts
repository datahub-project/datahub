import { DataHubAiConversationActorType, DataHubAiConversationMessage, DataHubAiConversationMessageType } from '@types';

export const THINKING_WINDOW_MS = 2 * 60 * 1000; // 2 minutes

export function isUserOrAgentThinkingRecent(
    messages: DataHubAiConversationMessage[],
    now = Date.now(),
    windowMs = THINKING_WINDOW_MS,
): boolean {
    if (messages.length === 0) return false;
    const lastMessage = messages[messages.length - 1];
    const isUser = lastMessage.actor.type === DataHubAiConversationActorType.User;
    const isAgentThinking =
        lastMessage.actor.type === DataHubAiConversationActorType.Agent &&
        lastMessage.type === DataHubAiConversationMessageType.Thinking;
    if (!isUser && !isAgentThinking) return false;
    return lastMessage.time > now - windowMs;
}

/**
 * Predicate used by ChatArea polling: should we poll while waiting for a response?
 * True when not streaming AND there is a recent user message or agent thinking message.
 */
export function shouldPollForThinking({
    messages,
    isStreaming,
    now = Date.now(),
    windowMs = THINKING_WINDOW_MS,
}: {
    messages: DataHubAiConversationMessage[];
    isStreaming: boolean;
    now?: number;
    windowMs?: number;
}): boolean {
    if (isStreaming) return false;
    return isUserOrAgentThinkingRecent(messages, now, windowMs);
}
