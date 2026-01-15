import { useEffect, useMemo, useRef, useState } from 'react';

import analytics, { ChatLocationType, EventType } from '@app/analytics';
import { MessageContext, useChatStream } from '@app/chat/hooks/useChatStream';
import { createUserMessage, emitMessageAnalytics } from '@app/chat/utils/chatMessageUtils';
import { groupMessages } from '@app/chat/utils/messageGrouping';
import { isUserOrAgentThinkingRecent } from '@app/chat/utils/thinkingState';

import { DataHubAiConversationActorType, DataHubAiConversationMessage, DataHubAiConversationMessageType } from '@types';

export interface UseChatMessagesProps {
    conversationUrn: string;
    userUrn: string;
    onMessageReceived?: (message: DataHubAiConversationMessage) => void;
    onStreamComplete?: () => void;
    agentName?: string;
    chatLocation: ChatLocationType;
}

export interface UseChatMessagesReturn {
    messages: DataHubAiConversationMessage[];
    setMessages: React.Dispatch<React.SetStateAction<DataHubAiConversationMessage[]>>;
    messageGroups: ReturnType<typeof groupMessages>;
    isStreaming: boolean;
    currentMessage: DataHubAiConversationMessage | null;
    handleSendMessage: (text: string, convoUrn?: string, messageContext?: MessageContext) => void;
    handleStopStreaming: () => void;
    messagesEndRef: React.RefObject<HTMLDivElement>;
}

/**
 * Hook to manage chat messages, streaming, and analytics.
 * Consolidates shared logic between ChatArea and EmbeddedChat.
 */
export const useChatMessages = ({
    conversationUrn,
    userUrn,
    onMessageReceived,
    onStreamComplete,
    agentName,
    chatLocation,
}: UseChatMessagesProps): UseChatMessagesReturn => {
    const [messages, setMessages] = useState<DataHubAiConversationMessage[]>([]);
    const messagesEndRef = useRef<HTMLDivElement>(null);
    const hasInitialScrolledRef = useRef(false);

    const { sendMessage, stopStreaming, isStreaming, currentMessage } = useChatStream({
        conversationUrn,
        onMessageReceived: (message: DataHubAiConversationMessage) => {
            // Skip USER messages since we add them optimistically
            if (message.actor.type === DataHubAiConversationActorType.User) {
                return;
            }

            setMessages((prev) => {
                // Check if this message is already in the list (to avoid duplicates)
                const exists = prev.some((m) => m.time === message.time && m.content.text === message.content.text);
                if (exists) {
                    return prev;
                }
                return [...prev, message];
            });

            // Call custom onMessageReceived if provided
            if (onMessageReceived) {
                onMessageReceived(message);
            }
        },
        onStreamComplete,
        agentName,
        chatLocation,
    });

    // Determine if we should show a "Thinking..." placeholder
    // This handles the case where user navigates away and returns mid-stream:
    // the stream is gone, but we show "Thinking..." until the response arrives or times out
    const shouldShowThinkingPlaceholder = useMemo(
        () => !isStreaming && isUserOrAgentThinkingRecent(messages),
        [messages, isStreaming],
    );

    // Group messages for rendering
    const messageGroups = useMemo(() => {
        const allMessages = [...messages];
        if (isStreaming && currentMessage) {
            allMessages.push(currentMessage);
        } else if (shouldShowThinkingPlaceholder) {
            // Show "Thinking..." placeholder when waiting for response
            const thinkingMessage: DataHubAiConversationMessage = {
                type: DataHubAiConversationMessageType.Thinking,
                time: Date.now(),
                actor: { type: DataHubAiConversationActorType.Agent },
                content: { text: '' },
            };
            allMessages.push(thinkingMessage);
        }
        return groupMessages(allMessages);
    }, [messages, currentMessage, isStreaming, shouldShowThinkingPlaceholder]);

    // Scroll to bottom when messages change or streaming
    // Handles both initial load (instant) and ongoing updates (smooth)
    useEffect(() => {
        // Detect initial load: first time we have messages
        const isInitialLoad = !hasInitialScrolledRef.current && messages.length > 0;

        if (isInitialLoad) {
            // Initial load: jump immediately to bottom with a delay for DOM updates
            hasInitialScrolledRef.current = true;
            setTimeout(() => {
                messagesEndRef.current?.scrollIntoView({ behavior: 'auto' });
            }, 100);
        } else if (messages.length > 0 || isStreaming) {
            // Ongoing updates: smooth scroll
            messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
        }
    }, [messages, currentMessage, isStreaming]);

    // Reset initial scroll flag when conversation changes
    useEffect(() => {
        hasInitialScrolledRef.current = false;
    }, [conversationUrn]);

    // Cleanup: stop streaming when conversation changes or component unmounts
    useEffect(() => {
        return () => {
            stopStreaming();
        };
    }, [conversationUrn, stopStreaming]);

    const handleSendMessage = (text: string, convoUrn?: string, messageContext?: MessageContext) => {
        if (!text.trim()) {
            return;
        }

        // Add user message to the list immediately (optimistic update)
        const userMessage = createUserMessage(text, userUrn);
        setMessages((prev) => [...prev, userMessage]);

        // Calculate message index - count existing user messages
        const userMessageCount = messages.filter(
            (msg) => msg.actor.type === DataHubAiConversationActorType.User,
        ).length;

        // Emit analytics event for message creation
        emitMessageAnalytics(convoUrn || conversationUrn, text, userMessageCount, messages.length, chatLocation);

        // Send the message with optional message context
        sendMessage(text, convoUrn, messageContext);
    };

    const handleStopStreaming = () => {
        // Emit analytics event for stopping chat response
        analytics.event({
            type: EventType.StopDataHubChatResponseEvent,
            conversationUrn,
        });
        stopStreaming();
    };

    return {
        messages,
        setMessages,
        messageGroups,
        isStreaming,
        currentMessage,
        handleSendMessage,
        handleStopStreaming,
        messagesEndRef,
    };
};
