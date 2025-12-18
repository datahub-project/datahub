import { DataHubAiConversationMessage, DataHubAiConversationMessageType } from '@types';

/**
 * Helper to check if message is thinking/tool related.
 * These intermediate/internal messages are filtered out from the main conversation flow
 * to provide a cleaner UX, showing only the user-AI text exchange.
 */
export const isThinkingOrToolMessage = (message: DataHubAiConversationMessage): boolean => {
    return (
        message.type === DataHubAiConversationMessageType.Thinking ||
        message.type === DataHubAiConversationMessageType.ToolCall ||
        message.type === DataHubAiConversationMessageType.ToolResult
    );
};

/**
 * Simplifies the conversation view by hiding internal processing steps that would
 * distract users from the actual Q&A exchange and clutter the chat interface.
 */
export const extractTextMessages = (messages: DataHubAiConversationMessage[]): DataHubAiConversationMessage[] => {
    return messages.filter((msg) => !isThinkingOrToolMessage(msg));
};

/**
 * Uses natural key deduplication (timestamp + content) because messages lack unique IDs in the schema.
 * This prevents showing duplicate messages when streaming updates overlap with polling/refetch cycles.
 */
export const shouldAddMessage = (
    existingMessages: DataHubAiConversationMessage[],
    newMessage: DataHubAiConversationMessage,
): boolean => {
    const newText = newMessage.content?.text;
    return !existingMessages.some((m) => m.time === newMessage.time && m.content?.text === newText);
};

/**
 * Group consecutive thinking/tool messages together
 */
export type MessageGroup =
    | { type: 'thinking'; messages: DataHubAiConversationMessage[] }
    | { type: 'regular'; message: DataHubAiConversationMessage };

export const groupMessages = (messages: DataHubAiConversationMessage[]): MessageGroup[] => {
    const groups: MessageGroup[] = [];
    let currentThinkingGroup: DataHubAiConversationMessage[] = [];

    messages.forEach((message) => {
        if (isThinkingOrToolMessage(message)) {
            currentThinkingGroup.push(message);
        } else {
            // If we have accumulated thinking messages, save the group
            if (currentThinkingGroup.length > 0) {
                groups.push({ type: 'thinking', messages: currentThinkingGroup });
                currentThinkingGroup = [];
            }
            // Add regular message
            groups.push({ type: 'regular', message });
        }
    });

    // Don't forget remaining thinking messages
    if (currentThinkingGroup.length > 0) {
        groups.push({ type: 'thinking', messages: currentThinkingGroup });
    }

    return groups;
};
