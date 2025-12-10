import { DataHubAiConversationMessage, DataHubAiConversationMessageType } from '@types';

/**
 * Helper to check if message is thinking/tool related
 */
export const isThinkingOrToolMessage = (message: DataHubAiConversationMessage): boolean => {
    return (
        message.type === DataHubAiConversationMessageType.Thinking ||
        message.type === DataHubAiConversationMessageType.ToolCall ||
        message.type === DataHubAiConversationMessageType.ToolResult
    );
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
