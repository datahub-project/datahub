import analytics, { EventType } from '@app/analytics';
import { extractReferencesFromMarkdown } from '@app/chat/utils/extractUrnsFromMarkdown';

import { DataHubAiConversationActorType, DataHubAiConversationMessage, DataHubAiConversationMessageType } from '@types';

/**
 * Creates a user message object for optimistic UI updates
 */
export const createUserMessage = (text: string, userUrn: string): DataHubAiConversationMessage => {
    return {
        type: DataHubAiConversationMessageType.Text,
        time: Date.now(),
        actor: {
            type: DataHubAiConversationActorType.User,
            actor: userUrn,
        },
        content: {
            text,
        },
    };
};

/**
 * Emits analytics event for message creation
 */
export const emitMessageAnalytics = (
    conversationUrn: string,
    messageText: string,
    userMessageIndex: number,
    totalMessageCount: number,
): void => {
    const mentions = extractReferencesFromMarkdown(messageText);

    analytics.event({
        type: EventType.CreateDataHubChatMessageEvent,
        conversationUrn,
        messageLength: messageText.length,
        hasEntityMentions: mentions.length > 0,
        entityMentionCount: mentions.length,
        userMessageIndex,
        totalMessageCount,
        messagePreview: messageText.substring(0, 200),
    });
};
