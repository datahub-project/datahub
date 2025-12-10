import { Loader, Text, colors } from '@components';
import { ChatCircle } from '@phosphor-icons/react';
import React, { useEffect, useRef, useState } from 'react';
import styled, { useTheme } from 'styled-components';

import { MessageList } from '@app/chat/components/MessageList';
import { SuggestedQuestions } from '@app/chat/components/SuggestedQuestions';
import { ChatInput } from '@app/chat/components/input/ChatInput';
import { useChatMessages } from '@app/chat/hooks/useChatMessages';
import { ChatFeatureFlags } from '@app/chat/types';
import { useAppConfig } from '@app/useAppConfig';

import { useGetDataHubAiConversationQuery } from '@graphql/aiChat.generated';
import { Entity } from '@types';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    min-height: 0;
    flex: 1;
    overflow: hidden;
`;

const Header = styled.div`
    padding: 16px 24px;
    border-bottom: 1px solid ${colors.gray[100]};
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const HeaderTitle = styled.div`
    max-width: 100%;
`;

const ContentWrapper = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    min-height: 0;
    overflow: hidden;
`;

const MessagesContainer = styled.div`
    flex: 1;
    min-height: 0;
    overflow-y: auto;
    display: flex;
    justify-content: center;
`;

const MessagesContent = styled.div`
    width: 100%;
    max-width: 60%;
    padding: 24px 12px;
    min-height: 100%;
    display: flex;
    flex-direction: column;

    @media (max-width: 1400px) {
        max-width: 70%;
    }

    @media (max-width: 1200px) {
        max-width: 80%;
    }

    @media (max-width: 1000px) {
        max-width: 90%;
    }

    @media (max-width: 800px) {
        max-width: 100%;
    }
`;

const InputContainer = styled.div`
    display: flex;
    justify-content: center;
    padding: 16px 0;
`;

const InputContent = styled.div`
    width: 100%;
    max-width: 60%;

    @media (max-width: 1400px) {
        max-width: 70%;
    }

    @media (max-width: 1200px) {
        max-width: 80%;
    }

    @media (max-width: 1000px) {
        max-width: 90%;
    }

    @media (max-width: 800px) {
        max-width: 100%;
    }
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
    const hasAutoSentInitialMessage = useRef(false);
    const appConfig = useAppConfig();
    const themeConfig = useTheme();

    // Fetch conversation data
    const { data, loading, refetch } = useGetDataHubAiConversationQuery({
        variables: { urn: conversationUrn },
        fetchPolicy: 'cache-and-network',
    });

    const conversation = data?.getDataHubAiConversation;

    // Use shared chat messages hook
    const {
        messages,
        setMessages,
        messageGroups,
        isStreaming,
        handleSendMessage,
        handleStopStreaming,
        messagesEndRef,
    } = useChatMessages({
        conversationUrn,
        userUrn,
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
    }, [conversation, setMessages]);

    // Auto-send initial message if provided (from SearchBar "Ask DataHub")
    useEffect(() => {
        if (initialMessage && !hasAutoSentInitialMessage.current && !loading && conversation) {
            hasAutoSentInitialMessage.current = true;
            setInputValue(initialMessage);
            // Auto-send after a brief delay to ensure everything is ready
            setTimeout(() => {
                handleSendMessage(initialMessage);
                setInputValue('');
            }, 100);
        }
    }, [initialMessage, loading, conversation, handleSendMessage]);

    const handleSend = () => {
        if (!inputValue.trim()) {
            return;
        }

        handleSendMessage(inputValue);
        setInputValue('');
    };

    const handleQuestionSelect = (question: string) => {
        if (isStreaming) return;

        handleSendMessage(question);
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
                <HeaderTitle>
                    <Text size="md" weight="bold" style={{ color: colors.gray[600] }}>
                        {conversation.title || 'New Chat'}
                    </Text>
                </HeaderTitle>
            </Header>
            <ContentWrapper>
                <MessagesContainer>
                    <MessagesContent>
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
                                        onStop={handleStopStreaming}
                                        placeholder="Ask anything about your data..."
                                        isStreaming={isStreaming}
                                        isWelcomeState
                                    />
                                    <SuggestedQuestions onQuestionSelect={handleQuestionSelect} />
                                </EmptyStateInputWrapper>
                            </EmptyState>
                        ) : (
                            <>
                                <MessageList
                                    messageGroups={messageGroups}
                                    verboseMode={featureFlags.verboseMode}
                                    isStreaming={isStreaming}
                                    selectedEntityUrn={selectedEntityUrn}
                                    onEntitySelect={onEntitySelect}
                                />
                                <div ref={messagesEndRef} />
                            </>
                        )}
                    </MessagesContent>
                </MessagesContainer>
                {messages.length > 0 && (
                    <InputContainer>
                        <InputContent>
                            <ChatInput
                                value={inputValue}
                                onChange={setInputValue}
                                onSubmit={handleSend}
                                onStop={handleStopStreaming}
                                placeholder="Ask about your data... (use @ to mention assets)"
                                isStreaming={isStreaming}
                            />
                        </InputContent>
                    </InputContainer>
                )}
            </ContentWrapper>
        </Container>
    );
};
