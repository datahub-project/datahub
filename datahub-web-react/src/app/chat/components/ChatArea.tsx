import { Loader, Text, colors } from '@components';
import { ChatCircle } from '@phosphor-icons/react';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import styled, { useTheme } from 'styled-components';

import { ChatLocationType } from '@app/analytics';
import FreeTrialAIChatPopover from '@app/chat/FreeTrialAIChatPopover';
import { MessageList } from '@app/chat/components/MessageList';
import { SuggestedQuestions } from '@app/chat/components/SuggestedQuestions';
import { ChatInput } from '@app/chat/components/input/ChatInput';
import { useChatMessages } from '@app/chat/hooks/useChatMessages';
import { ChatFeatureFlags, ChatMessageAction, ChatVariant } from '@app/chat/types';
import { removeMarkdown } from '@app/entityV2/shared/components/styled/StripMarkdownText';
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

const MessagesContainer = styled.div<{ $variant?: ChatVariant }>`
    flex: 1;
    min-height: 0;
    overflow-y: auto;
    overflow-x: ${(props) => (props.$variant === ChatVariant.Compact ? 'hidden' : 'visible')};
    display: flex;
    flex-direction: ${(props) => (props.$variant === ChatVariant.Compact ? 'column' : 'row')};
    justify-content: ${(props) => (props.$variant === ChatVariant.Compact ? 'flex-start' : 'center')};
    /* Reduce right padding slightly in compact to account for scrollbar space */
    padding: ${(props) => (props.$variant === ChatVariant.Compact ? '20px 12px 20px 20px' : '0')};
    gap: ${(props) => (props.$variant === ChatVariant.Compact ? '32px' : '0')};
    width: 100%;
    max-width: 100%;
    min-width: 0;

    /* Compact: hide scrollbars until hover to match prior sidebar UX */
    ${(props) =>
        props.$variant === ChatVariant.Compact
            ? `
        &::-webkit-scrollbar {
            width: 6px;
            opacity: 0;
            transition: opacity 0.2s;
        }

        &::-webkit-scrollbar-track {
            background: transparent;
        }

        &::-webkit-scrollbar-thumb {
            background-color: transparent;
            border-radius: 3px;
            transition: background-color 0.2s;
        }

        &:hover::-webkit-scrollbar-thumb {
            background-color: ${colors.gray[200]};
        }

        &::-webkit-scrollbar-thumb:hover {
            background-color: ${colors.gray[300]};
        }

        scrollbar-width: thin;
        scrollbar-color: transparent transparent;

        &:hover {
            scrollbar-color: ${colors.gray[200]} transparent;
        }
    `
            : ''}
`;

const MessagesContent = styled.div<{ $variant?: ChatVariant }>`
    width: 100%;
    max-width: ${(props) => (props.$variant === ChatVariant.Compact ? '100%' : '60%')};
    padding: ${(props) => (props.$variant === ChatVariant.Compact ? '0' : '24px 12px')};
    min-height: 100%;
    display: flex;
    flex-direction: column;
    gap: 0;

    @media (max-width: 1400px) {
        max-width: ${(props) => (props.$variant === ChatVariant.Compact ? '100%' : '70%')};
    }

    @media (max-width: 1200px) {
        max-width: ${(props) => (props.$variant === ChatVariant.Compact ? '100%' : '80%')};
    }

    @media (max-width: 1000px) {
        max-width: 90%;
    }

    @media (max-width: 800px) {
        max-width: 100%;
    }
`;

const InputContainer = styled.div<{ $variant?: ChatVariant }>`
    display: flex;
    flex-direction: ${(props) => (props.$variant === ChatVariant.Compact ? 'column' : 'row')};
    justify-content: ${(props) => (props.$variant === ChatVariant.Compact ? 'flex-start' : 'center')};
    padding: ${(props) => (props.$variant === ChatVariant.Compact ? '12px 16px 16px 16px' : '16px 0')};
    background: ${(props) => (props.$variant === ChatVariant.Compact ? 'white' : 'transparent')};
    gap: ${(props) => (props.$variant === ChatVariant.Compact ? '8px' : '0')};
`;

const InputContent = styled.div<{ $variant?: ChatVariant }>`
    width: 100%;
    max-width: ${(props) => (props.$variant === ChatVariant.Compact ? '100%' : '60%')};

    @media (max-width: 1400px) {
        max-width: ${(props) => (props.$variant === ChatVariant.Compact ? '100%' : '70%')};
    }

    @media (max-width: 1200px) {
        max-width: ${(props) => (props.$variant === ChatVariant.Compact ? '100%' : '80%')};
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

export const EmptyStateContainer = styled.div<{ $variant?: ChatVariant }>`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: ${(props) => (props.$variant === ChatVariant.Compact ? '24px' : '40px')};
    height: 100%;
    margin-top: ${(props) => (props.$variant === ChatVariant.Compact ? '0' : '-100px')};
`;

export const EmptyStateLogoContainer = styled.div`
    margin-bottom: 20px;

    svg {
        width: 64px;
        height: 64px;
    }
`;

export const EmptyStateTitle = styled.div`
    font-size: 20px;
    font-weight: 600;
    color: ${colors.gray[600]};
    margin: 0 0 20px 0;
    text-align: center;
`;

export const EmptyStateInputWrapper = styled.div<{ $variant?: ChatVariant }>`
    width: 100%;
    max-width: ${(props) => (props.$variant === ChatVariant.Compact ? '100%' : '700px')};
`;

export const EmptyStateSuggestionsContainer = styled.div<{ $variant?: ChatVariant }>`
    display: flex;
    flex-direction: column;
    gap: 4px;
    justify-content: center;
    align-items: center;
    margin-top: 24px;
    width: 100%;
    max-width: ${(props) => (props.$variant === ChatVariant.Compact ? '100%' : '700px')};
`;

export const EmptyStatePillsWrapper = styled.div<{ $variant?: ChatVariant }>`
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    justify-content: center;
    width: 100%;
`;

interface ChatAreaProps {
    conversationUrn?: string;
    userUrn: string;
    featureFlags: ChatFeatureFlags;
    onConversationUpdate?: () => void;
    onConversationNotFound?: () => void;
    setTitle?: (title: string) => void;
    /** Title from parent (for optimistic updates) */
    title?: string;
    /** Draft text for the current conversation (managed by parent) */
    draft?: string;
    /** Persist draft text for the given conversation URN */
    onDraftChange?: (conversationUrn: string, draft: string) => void;
    selectedEntityUrn?: string;
    onEntitySelect?: (entity: Entity | null) => void;
    initialMessage?: string;
    variant?: ChatVariant;
    messageActions?: ChatMessageAction[];
    showReferences?: boolean;
    suggestedQuestions?: string[];
    /** Called when user submits a message but no conversation exists yet */
    onStartConversation?: (message: string) => Promise<boolean>;
    /** Placeholder text for the input field in welcome state */
    welcomePlaceholder?: string;
    chatLocation: ChatLocationType;
}

/** Props for the inner component that requires a conversation */
interface ChatAreaWithConversationProps extends Omit<ChatAreaProps, 'conversationUrn' | 'onStartConversation'> {
    conversationUrn: string;
}

/**
 * Inner component that renders when a conversation exists.
 * This allows us to call useChatMessages only when we have a valid conversationUrn.
 */
const ChatAreaWithConversation: React.FC<ChatAreaWithConversationProps> = ({
    conversationUrn,
    userUrn,
    featureFlags,
    onConversationUpdate,
    onConversationNotFound,
    setTitle,
    title: titleFromParent,
    selectedEntityUrn,
    onEntitySelect,
    initialMessage,
    variant = ChatVariant.Full,
    messageActions,
    showReferences = true,
    suggestedQuestions,
    welcomePlaceholder = 'Ask anything about your data...',
    draft,
    onDraftChange,
    chatLocation,
}) => {
    const [inputValue, setInputValue] = useState('');
    const hasAutoSentInitialMessage = useRef(false);
    const appConfig = useAppConfig();
    const themeConfig = useTheme();

    const updateInputValue = useCallback(
        (value: string) => {
            setInputValue(value);
            if (onDraftChange) {
                onDraftChange(conversationUrn, value);
            }
        },
        [conversationUrn, onDraftChange],
    );

    // Clear input when switching conversations to avoid leaking previous text
    useEffect(() => {
        const nextValue = draft ?? '';
        if (inputValue !== nextValue) {
            updateInputValue(nextValue);
        }
    }, [conversationUrn, draft, inputValue, updateInputValue]);

    // Fetch conversation data
    const { data, loading, refetch } = useGetDataHubAiConversationQuery({
        variables: { urn: conversationUrn },
        fetchPolicy: 'cache-and-network',
    });

    const conversation = data?.getDataHubAiConversation;

    // Use shared chat messages hook - only called when conversationUrn exists
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
            refetch();
            // Refetch conversation list to get updated title from server
            if (onConversationUpdate) {
                onConversationUpdate();
            }
        },
        chatLocation,
    });

    // Initialize messages from conversation
    useEffect(() => {
        if (conversation?.messages) {
            setMessages(conversation.messages);
        }
    }, [conversation, setMessages]);

    // Notify parent after fetch completes if conversation is missing
    useEffect(() => {
        if (!loading && !conversation && onConversationNotFound) {
            onConversationNotFound();
        }
    }, [loading, conversation, onConversationNotFound]);

    // Auto-send initial message if provided (from SearchBar "Ask DataHub")
    useEffect(() => {
        if (initialMessage && !hasAutoSentInitialMessage.current && !loading && conversation) {
            hasAutoSentInitialMessage.current = true;
            updateInputValue(initialMessage);
            setTimeout(() => {
                // Update title optimistically for auto-sent messages (same as handleSend)
                if (messages.length === 0 && setTitle) {
                    const title =
                        initialMessage.length > 100 ? `${initialMessage.substring(0, 100)}...` : initialMessage;
                    setTitle(title);
                }
                handleSendMessage(initialMessage);
                updateInputValue('');
            }, 100);
        }
    }, [initialMessage, loading, conversation, handleSendMessage, messages.length, setTitle, updateInputValue]);

    const handleSend = () => {
        if (!inputValue.trim()) {
            return;
        }
        // Update title optimistically on first message
        if (messages.length === 0 && setTitle) {
            const title = inputValue.length > 100 ? `${inputValue.substring(0, 100)}...` : inputValue;
            setTitle(title);
        }
        handleSendMessage(inputValue);
        updateInputValue('');
    };

    const handleQuestionSelect = (question: string) => {
        if (isStreaming) return;
        // Update title optimistically on first message
        if (messages.length === 0 && setTitle) {
            const title = question.length > 100 ? `${question.substring(0, 100)}...` : question;
            setTitle(title);
        }
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
                <EmptyStateContainer $variant={variant}>
                    <Text color="gray">Conversation not found</Text>
                </EmptyStateContainer>
            </Container>
        );
    }

    const showEmptyState = messages.length === 0;

    return (
        <Container>
            {variant !== ChatVariant.Compact && (
                <Header>
                    <HeaderTitle>
                        <Text size="md" weight="bold" style={{ color: colors.gray[600] }}>
                            {removeMarkdown(titleFromParent || conversation.title || 'New Chat')}
                        </Text>
                    </HeaderTitle>
                </Header>
            )}
            <ContentWrapper>
                <MessagesContainer $variant={variant}>
                    <MessagesContent $variant={variant}>
                        {showEmptyState ? (
                            <EmptyStateContainer $variant={variant}>
                                <EmptyStateLogoContainer>
                                    {appConfig.config?.visualConfig?.logoUrl || themeConfig?.assets?.logoUrl ? (
                                        <img
                                            src={appConfig.config.visualConfig.logoUrl || themeConfig.assets.logoUrl}
                                            alt="DataHub"
                                            style={{ width: '40px', height: '40px', objectFit: 'contain' }}
                                        />
                                    ) : (
                                        <ChatCircle size={64} weight="duotone" color="#1890ff" />
                                    )}
                                </EmptyStateLogoContainer>
                                <EmptyStateTitle>What can we help with today?</EmptyStateTitle>
                                <EmptyStateInputWrapper $variant={variant}>
                                    <ChatInput
                                        value={inputValue}
                                        onChange={updateInputValue}
                                        onSubmit={handleSend}
                                        onStop={handleStopStreaming}
                                        placeholder={welcomePlaceholder}
                                        isStreaming={isStreaming}
                                        isWelcomeState
                                        autoFocus
                                    />
                                    <SuggestedQuestions
                                        onQuestionSelect={handleQuestionSelect}
                                        questions={suggestedQuestions}
                                    />
                                    <FreeTrialAIChatPopover variant="welcome" />
                                </EmptyStateInputWrapper>
                            </EmptyStateContainer>
                        ) : (
                            <>
                                <MessageList
                                    messageGroups={messageGroups}
                                    verboseMode={featureFlags.verboseMode}
                                    isStreaming={isStreaming}
                                    variant={variant}
                                    messageActions={messageActions}
                                    showReferences={showReferences}
                                    conversationUrn={conversationUrn}
                                    selectedEntityUrn={selectedEntityUrn}
                                    onEntitySelect={onEntitySelect}
                                />
                                <div ref={messagesEndRef} />
                            </>
                        )}
                    </MessagesContent>
                </MessagesContainer>
                {!showEmptyState && (
                    <InputContainer $variant={variant}>
                        <InputContent $variant={variant}>
                            <ChatInput
                                value={inputValue}
                                onChange={updateInputValue}
                                onSubmit={handleSend}
                                onStop={handleStopStreaming}
                                placeholder="Ask about your data... (use @ to mention assets)"
                                isStreaming={isStreaming}
                                autoFocus
                            />
                        </InputContent>
                    </InputContainer>
                )}
            </ContentWrapper>
            {messages.length > 1 && !isStreaming && <FreeTrialAIChatPopover variant="completion" />}
        </Container>
    );
};

/**
 * Public ChatArea component that handles both states:
 * 1. No conversation yet - shows welcome state with onStartConversation callback
 * 2. Has conversation - delegates to ChatAreaWithConversation
 */
export const ChatArea: React.FC<ChatAreaProps> = ({
    conversationUrn,
    onStartConversation,
    variant = ChatVariant.Full,
    suggestedQuestions,
    welcomePlaceholder = 'Ask anything about your data...',
    ...rest
}) => {
    const [inputValue, setInputValue] = useState('');
    const [isStarting, setIsStarting] = useState(false);
    const appConfig = useAppConfig();
    const themeConfig = useTheme();

    // If we have a conversation, render the full chat experience
    if (conversationUrn) {
        return (
            <ChatAreaWithConversation
                conversationUrn={conversationUrn}
                variant={variant}
                suggestedQuestions={suggestedQuestions}
                welcomePlaceholder={welcomePlaceholder}
                onConversationNotFound={rest.onConversationNotFound}
                {...rest}
            />
        );
    }

    // No conversation yet - show welcome state
    const handleSend = async () => {
        if (!inputValue.trim() || !onStartConversation) {
            return;
        }
        setIsStarting(true);
        const success = await onStartConversation(inputValue);
        if (success) {
            setInputValue('');
        }
        setIsStarting(false);
    };

    const handleQuestionSelect = async (question: string) => {
        if (isStarting || !onStartConversation) return;
        setIsStarting(true);
        await onStartConversation(question);
        setIsStarting(false);
    };

    return (
        <Container>
            <ContentWrapper>
                <MessagesContainer $variant={variant}>
                    <MessagesContent $variant={variant}>
                        <EmptyStateContainer $variant={variant}>
                            <EmptyStateLogoContainer>
                                {appConfig.config?.visualConfig?.logoUrl || themeConfig?.assets?.logoUrl ? (
                                    <img
                                        src={appConfig.config.visualConfig.logoUrl || themeConfig.assets.logoUrl}
                                        alt="DataHub"
                                        style={{ width: '40px', height: '40px', objectFit: 'contain' }}
                                    />
                                ) : (
                                    <ChatCircle size={64} weight="duotone" color="#1890ff" />
                                )}
                            </EmptyStateLogoContainer>
                            <EmptyStateTitle>What can we help with today?</EmptyStateTitle>
                            <EmptyStateInputWrapper $variant={variant}>
                                <ChatInput
                                    value={inputValue}
                                    onChange={setInputValue}
                                    onSubmit={handleSend}
                                    placeholder={welcomePlaceholder}
                                    isStreaming={isStarting}
                                    isWelcomeState
                                />
                                <SuggestedQuestions
                                    onQuestionSelect={handleQuestionSelect}
                                    questions={suggestedQuestions}
                                />
                                {variant === ChatVariant.Full && <FreeTrialAIChatPopover variant="welcome" />}
                            </EmptyStateInputWrapper>
                        </EmptyStateContainer>
                    </MessagesContent>
                </MessagesContainer>
            </ContentWrapper>
        </Container>
    );
};
