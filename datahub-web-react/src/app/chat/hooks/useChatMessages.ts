import { useEffect, useMemo, useRef, useState } from 'react';

import analytics, { EventType } from '@app/analytics';
import { MessageContext, useChatStream } from '@app/chat/hooks/useChatStream';
import { createUserMessage, emitMessageAnalytics } from '@app/chat/utils/chatMessageUtils';
import { groupMessages } from '@app/chat/utils/messageGrouping';

import { DataHubAiConversationActorType, DataHubAiConversationMessage } from '@types';

export interface UseChatMessagesProps {
    conversationUrn: string;
    userUrn: string;
    onMessageReceived?: (message: DataHubAiConversationMessage) => void;
    onStreamComplete?: () => void;
    agentName?: string;
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
    });

    // Group messages for rendering
    const messageGroups = useMemo(() => {
        const allMessages = [...messages];
        if (isStreaming && currentMessage) {
            allMessages.push(currentMessage);
        }
        return groupMessages(allMessages);
    }, [messages, currentMessage, isStreaming]);

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

    // Cleanup: Stop streaming when conversation changes or component unmounts
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
        emitMessageAnalytics(convoUrn || conversationUrn, text, userMessageCount, messages.length);

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
