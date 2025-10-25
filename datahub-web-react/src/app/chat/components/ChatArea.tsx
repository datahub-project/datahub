import { Button, Loader, Text } from '@components';
import { ChatCircle } from '@phosphor-icons/react';
import React, { useEffect, useMemo, useRef, useState } from 'react';
import styled, { useTheme } from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { SuggestedQuestions } from '@app/chat/components/SuggestedQuestions';
import { ChatInput } from '@app/chat/components/input/ChatInput';
import { ChatMessage } from '@app/chat/components/messages/ChatMessage';
import { ThinkingGroup } from '@app/chat/components/messages/ThinkingGroup';
import { TypingIndicator } from '@app/chat/components/messages/TypingIndicator';
import { useChatStream } from '@app/chat/hooks/useChatStream';
import { ChatFeatureFlags } from '@app/chat/types';
import { extractReferencesFromMarkdown } from '@app/chat/utils/extractUrnsFromMarkdown';
import { useAppConfig } from '@app/useAppConfig';

import { useGetDataHubAiConversationQuery } from '@graphql/aiChat.generated';
import {
    DataHubAiConversationActorType,
    DataHubAiConversationMessage,
    DataHubAiConversationMessageType,
    Entity,
} from '@types';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    min-height: 0;
    flex: 1;
`;

const Header = styled.div`
    padding: 16px 24px;
    border-bottom: 1px solid #e0e0e0;
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const MessagesContainer = styled.div`
    flex: 1;
    min-height: 0;
    overflow-y: auto;
    padding: 24px;
`;

const InputContainer = styled.div`
    padding: 16px 24px;
    border-top: 1px solid #e0e0e0;
`;

const InputWrapper = styled.div`
    display: flex;
    gap: 12px;
    align-items: center;
`;

const LoadingContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    flex: 1;
    gap: 16px;
`;

const EmptyState = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 40px;
    height: 100%;
    margin-top: -100px;
`;

const LogoContainer = styled.div`
    margin-bottom: 20px;

    svg {
        width: 64px;
        height: 64px;
    }
`;

const WelcomeTitle = styled.div`
    font-size: 20px;
    font-weight: 600;
    color: #262626;
    margin: 0 0 20px 0;
    text-align: center;
`;

const EmptyStateInputWrapper = styled.div`
    width: 100%;
    max-width: 700px;
`;

interface ChatAreaProps {
    conversationUrn: string;
    userUrn: string;
    featureFlags: ChatFeatureFlags;
    onConversationUpdate?: () => void;
    selectedEntityUrn?: string;
    onEntitySelect?: (entity: Entity | null) => void;
    initialMessage?: string;
}

// todo: extract to file with unit testing.
// Helper to check if message is thinking/tool related
const isThinkingOrToolMessage = (message: DataHubAiConversationMessage) => {
    return (
        message.type === DataHubAiConversationMessageType.Thinking ||
        message.type === DataHubAiConversationMessageType.ToolCall ||
        message.type === DataHubAiConversationMessageType.ToolResult
    );
};

// TODO: Extract to file with unit testing.
// Group consecutive thinking/tool messages together
type MessageGroup =
    | { type: 'thinking'; messages: DataHubAiConversationMessage[] }
    | { type: 'regular'; message: DataHubAiConversationMessage };

// Todo extract to file with unit testing.
const groupMessages = (messages: DataHubAiConversationMessage[]): MessageGroup[] => {
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

export const ChatArea: React.FC<ChatAreaProps> = ({
    conversationUrn,
    userUrn,
    featureFlags,
    onConversationUpdate,
    selectedEntityUrn,
    onEntitySelect,
    initialMessage,
}) => {
    const [inputValue, setInputValue] = useState('');
    const [messages, setMessages] = useState<DataHubAiConversationMessage[]>([]);
    const messagesEndRef = useRef<HTMLDivElement>(null);
    const hasAutoSentInitialMessage = useRef(false);
    const appConfig = useAppConfig();
    const themeConfig = useTheme();

    // Fetch conversation data
    const { data, loading, refetch } = useGetDataHubAiConversationQuery({
        variables: { urn: conversationUrn },
        fetchPolicy: 'cache-and-network',
    });

    const conversation = data?.getDataHubAiConversation;

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
        },
        onStreamComplete: () => {
            // Refetch conversation to get the complete message history
            refetch();
            if (onConversationUpdate) {
                onConversationUpdate(); // Notify parent to update conversation list
            }
        },
    });

    // Initialize messages from conversation
    useEffect(() => {
        if (conversation?.messages) {
            setMessages(conversation.messages);
        }
    }, [conversation]);

    // Scroll to bottom when conversation is first loaded
    useEffect(() => {
        if (conversation?.messages && conversation.messages.length > 0) {
            // Use a small delay to ensure the DOM has updated
            setTimeout(() => {
                messagesEndRef.current?.scrollIntoView({ behavior: 'auto' });
            }, 300);
        }
    }, [conversation]);

    // Cleanup: Stop streaming when conversation changes or component unmounts
    useEffect(() => {
        return () => {
            stopStreaming();
        };
    }, [conversationUrn, stopStreaming]);

    // Group messages for rendering
    const messageGroups = useMemo(() => {
        // Combine regular messages with streaming message if present
        const allMessages = [...messages];
        if (isStreaming && currentMessage) {
            allMessages.push(currentMessage);
        }
        return groupMessages(allMessages);
    }, [messages, currentMessage, isStreaming]);

    // Scroll to bottom when messages change or streaming
    useEffect(() => {
        messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    }, [messages, currentMessage, isStreaming]);

    // Auto-send initial message if provided (from SearchBar "Ask DataHub")
    useEffect(() => {
        if (initialMessage && !hasAutoSentInitialMessage.current && !loading && conversation) {
            hasAutoSentInitialMessage.current = true;
            setInputValue(initialMessage);
            // Auto-send after a brief delay to ensure everything is ready
            setTimeout(() => {
                // Add user message to the list immediately
                const userMessage: DataHubAiConversationMessage = {
                    type: DataHubAiConversationMessageType.Text,
                    time: Date.now(),
                    actor: {
                        type: DataHubAiConversationActorType.User,
                        actor: userUrn,
                    },
                    content: {
                        text: initialMessage,
                    },
                };
                setMessages((prev) => [...prev, userMessage]);

                // Send the message
                sendMessage(initialMessage);
                setInputValue('');
            }, 100);
        }
    }, [initialMessage, loading, conversation, userUrn, sendMessage]);

    const handleSend = () => {
        if (!inputValue.trim()) {
            return;
        }

        // Add user message to the list immediately
        const userMessage: DataHubAiConversationMessage = {
            type: DataHubAiConversationMessageType.Text,
            time: Date.now(),
            actor: {
                type: DataHubAiConversationActorType.User,
                actor: userUrn,
            },
            content: {
                text: inputValue,
            },
        };
        setMessages((prev) => [...prev, userMessage]);

        // Extract entity mentions from markdown
        const mentions = extractReferencesFromMarkdown(inputValue);

        // Emit analytics event for message creation
        analytics.event({
            type: EventType.CreateDataHubChatMessageEvent,
            conversationUrn,
            messageLength: inputValue.length,
            hasEntityMentions: mentions.length > 0,
            entityMentionCount: mentions.length,
        });

        // Send the message
        sendMessage(inputValue);
        setInputValue('');
    };

    const handleQuestionSelect = (question: string) => {
        if (isStreaming) return;

        // Add user message to the list immediately
        const userMessage: DataHubAiConversationMessage = {
            type: DataHubAiConversationMessageType.Text,
            time: Date.now(),
            actor: {
                type: DataHubAiConversationActorType.User,
                actor: userUrn,
            },
            content: {
                text: question,
            },
        };
        setMessages((prev) => [...prev, userMessage]);

        // Send the message
        sendMessage(question);
    };

    if (loading) {
        return (
            <Container>
                <LoadingContainer>
                    <Loader size="lg" />
                </LoadingContainer>
            </Container>
        );
    }

    if (!conversation) {
        return (
            <Container>
                <EmptyState>
                    <Text color="gray">Conversation not found</Text>
                </EmptyState>
            </Container>
        );
    }

    return (
        <Container>
            <Header>
                <Text size="lg" weight="bold">
                    {conversation.title || 'New Conversation'}
                </Text>
            </Header>
            <MessagesContainer>
                {messages.length === 0 ? (
                    <EmptyState>
                        <LogoContainer>
                            {appConfig.config?.visualConfig?.logoUrl || themeConfig?.assets?.logoUrl ? (
                                <img
                                    src={appConfig.config.visualConfig.logoUrl || themeConfig.assets.logoUrl}
                                    alt="DataHub"
                                    style={{ width: '40px', height: '40px', objectFit: 'contain' }}
                                />
                            ) : (
                                <ChatCircle size={64} weight="duotone" color="#1890ff" />
                            )}
                        </LogoContainer>
                        <WelcomeTitle>What can we help with today?</WelcomeTitle>
                        <EmptyStateInputWrapper>
                            <ChatInput
                                value={inputValue}
                                onChange={setInputValue}
                                onSubmit={handleSend}
                                placeholder="Ask anything about your data..."
                                disabled={isStreaming}
                            />
                            <SuggestedQuestions onQuestionSelect={handleQuestionSelect} />
                        </EmptyStateInputWrapper>
                    </EmptyState>
                ) : (
                    <>
                        {messageGroups.map((group, index) => {
                            if (group.type === 'thinking') {
                                const firstMessageTime = group.messages[0]?.time || index;
                                // Thinking group is complete if there's a next group (non-thinking) or if we're not streaming
                                const isComplete = index < messageGroups.length - 1 || !isStreaming;
                                return (
                                    <ThinkingGroup
                                        key={`thinking-group-${firstMessageTime}`}
                                        messages={group.messages}
                                        verboseMode={featureFlags.verboseMode}
                                        isComplete={isComplete}
                                    />
                                );
                            }
                            return (
                                <ChatMessage
                                    key={`${group.message.time}-${group.message.content.text.substring(0, 20)}`}
                                    message={group.message}
                                    selectedEntityUrn={selectedEntityUrn}
                                    onEntitySelect={onEntitySelect}
                                />
                            );
                        })}
                        {isStreaming && <TypingIndicator />}
                        <div ref={messagesEndRef} />
                    </>
                )}
            </MessagesContainer>
            {messages.length > 0 && (
                <InputContainer>
                    <InputWrapper>
                        <ChatInput
                            value={inputValue}
                            onChange={setInputValue}
                            onSubmit={handleSend}
                            placeholder="Ask about your data... (use @ to mention assets)"
                            disabled={isStreaming}
                        />
                        {!isStreaming ? (
                            <Button
                                icon={{
                                    icon: 'PaperPlaneRight',
                                    source: 'phosphor',
                                    weight: 'fill',
                                }}
                                onClick={handleSend}
                                isDisabled={!inputValue.trim() || isStreaming}
                            >
                                Send
                            </Button>
                        ) : (
                            <Button
                                icon={{
                                    icon: 'Stop',
                                    source: 'phosphor',
                                    weight: 'fill',
                                }}
                                onClick={() => {
                                    // Emit analytics event for stopping chat response
                                    analytics.event({
                                        type: EventType.StopDataHubChatResponseEvent,
                                        conversationUrn,
                                    });
                                    stopStreaming();
                                }}
                            >
                                Stop
                            </Button>
                        )}
                    </InputWrapper>
                </InputContainer>
            )}
        </Container>
    );
};
