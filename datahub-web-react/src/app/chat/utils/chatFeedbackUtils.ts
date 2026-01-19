import analytics, { ChatLocationType, ChatMessageReactionType, EventType } from '@app/analytics';

/**
 * Generates a unique message ID for tracking reactions.
 * Uses message timestamp and a preview of the content to create a stable identifier.
 */
export function generateMessageId(messageTime: number, messageText: string | undefined): string {
    const textPreview = (messageText || '').substring(0, 20);
    return `${messageTime}-${textPreview}`;
}

export interface EmitReactionEventParams {
    conversationUrn: string;
    messageId: string;
    reaction: ChatMessageReactionType;
    chatLocation: ChatLocationType;
    agentName?: string;
}

/**
 * Emits a chat message reaction analytics event.
 * Returns true if the event was emitted, false if missing required params.
 */
export function emitReactionEvent(params: EmitReactionEventParams): boolean {
    const { conversationUrn, messageId, reaction, chatLocation, agentName } = params;

    if (!conversationUrn || !chatLocation) {
        return false;
    }

    analytics.event({
        type: EventType.ChatMessageReactionEvent,
        conversationUrn,
        messageId,
        reaction,
        chatLocation,
        agentName,
    });

    return true;
}

export interface EmitFeedbackEventParams {
    conversationUrn: string;
    messageId: string;
    feedbackText: string;
    chatLocation: ChatLocationType;
    agentName?: string;
}

/**
 * Emits a chat message feedback analytics event.
 * Returns true if the event was emitted, false if missing required params.
 */
export function emitFeedbackEvent(params: EmitFeedbackEventParams): boolean {
    const { conversationUrn, messageId, feedbackText, chatLocation, agentName } = params;

    if (!conversationUrn || !chatLocation) {
        return false;
    }

    analytics.event({
        type: EventType.ChatMessageFeedbackEvent,
        conversationUrn,
        messageId,
        feedbackText,
        chatLocation,
        agentName,
    });

    return true;
}
