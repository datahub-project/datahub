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
import { useSyncChatMode } from '@app/chat/hooks/useSyncChatMode';
import { ChatFeatureFlags, ChatMessageAction, ChatVariant } from '@app/chat/types';
import { CHAT_MODE_OPTIONS, ChatMode, getAgentNameForChatMode } from '@app/chat/utils/chatModes';
import { shouldPollForThinking } from '@app/chat/utils/thinkingState';
import { removeMarkdown } from '@app/entityV2/shared/components/styled/StripMarkdownText';
import { useAppConfig, useIsAskDataHubModeSelectEnabled } from '@app/useAppConfig';

import { useGetDataHubAiConversationQuery } from '@graphql/aiChat.generated';
import { Entity } from '@types';

const truncateTitle = (text: string) => (text.length > 100 ? `${text.substring(0, 100)}...` : text);

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
    display: flex;
    flex-direction: column;
    padding: ${(props) => (props.$variant === ChatVariant.Compact ? '20px 12px 20px 20px' : '0')};
    width: 100%;
    max-width: 100%;
    min-width: 0;
    ${(props) => props.$variant === ChatVariant.Compact && 'overflow-x: hidden;'}
`;

const MessagesContent = styled.div<{ $variant?: ChatVariant }>`
    width: 100%;
    max-width: ${(props) => (props.$variant === ChatVariant.Compact ? '100%' : '800px')};
    margin: ${(props) => (props.$variant === ChatVariant.Compact ? '0' : '0 auto')};
    padding: ${(props) => (props.$variant === ChatVariant.Compact ? '0' : '24px 16px 0 16px')};
    min-height: 100%;
    display: flex;
    flex-direction: column;
    gap: 32px;
`;

const InputContainer = styled.div<{ $variant?: ChatVariant }>`
    display: flex;
    flex-direction: column;
    padding: ${(props) => (props.$variant === ChatVariant.Compact ? '12px 16px 16px 16px' : '16px')};
    gap: 8px;
    ${(props) => props.$variant === ChatVariant.Compact && 'background: white;'}
`;

const InputContent = styled.div<{ $variant?: ChatVariant }>`
    width: 100%;
    max-width: ${(props) => (props.$variant === ChatVariant.Compact ? '100%' : '800px')};
    margin: ${(props) => (props.$variant === ChatVariant.Compact ? '0' : '0 auto')};
`;

const LoadingContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    flex: 1;
    gap: 16px;
`;

const EmptyStateContainer = styled.div<{ $variant?: ChatVariant }>`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: ${(props) => (props.$variant === ChatVariant.Compact ? '24px' : '40px 40px 200px 40px')};
    height: 100%;
`;

const EmptyStateLogoContainer = styled.div`
    margin-bottom: 20px;

    svg {
        width: 64px;
        height: 64px;
    }
`;

const EmptyStateTitle = styled.div`
    font-size: 20px;
    font-weight: 600;
    color: ${colors.gray[600]};
    margin: 0 0 20px 0;
    text-align: center;
    text-wrap: auto;
`;

const EmptyStateInputWrapper = styled.div<{ $variant?: ChatVariant }>`
    width: 100%;
    max-width: ${(props) => (props.$variant === ChatVariant.Compact ? '100%' : '800px')};
`;

const ChatLogo = ({ logoUrl }: { logoUrl?: string | null }) =>
    logoUrl ? (
        <img src={logoUrl} alt="DataHub" style={{ width: '40px', height: '40px', objectFit: 'contain' }} />
    ) : (
        <ChatCircle size={64} weight="duotone" color="#1890ff" />
    );

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
    selectedMode: ChatMode;
    onModeChange: (mode: ChatMode) => void;
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
    selectedMode,
    onModeChange,
}) => {
    const [inputValue, setInputValue] = useState('');
    const hasAutoSentInitialMessage = useRef(false);
    const appConfig = useAppConfig();
    const themeConfig = useTheme();
    const isModeSelectEnabled = useIsAskDataHubModeSelectEnabled();

    const updateInputValue = useCallback(
        (value: string) => {
            setInputValue(value);
            if (onDraftChange) {
                onDraftChange(conversationUrn, value);
            }
        },
        [conversationUrn, onDraftChange],
    );

    // Sync input with draft when switching conversations to avoid leaking previous text.
    // IMPORTANT: Do not include inputValue in deps - that would cause every keystroke to
    // reset the input to empty (since draft is undefined in AskDataHubTab).
    // Sync input with draft when switching conversations to avoid leaking previous text.
    // IMPORTANT: Do not include inputValue in deps - that would cause every keystroke to
    // reset the input to empty (since draft is undefined in AskDataHubTab).
    useEffect(() => {
        const nextValue = draft ?? '';
        setInputValue(nextValue);
    }, [conversationUrn, draft, setInputValue]);

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
        agentName: getAgentNameForChatMode(selectedMode),
        chatLocation,
    });

    // Initialize messages from conversation
    useEffect(() => {
        if (conversation?.messages) {
            setMessages(conversation.messages);
        }
    }, [conversation, setMessages]);

    // Sync mode from last message's agentName when conversation loads or changes
    useSyncChatMode({
        conversationUrn,
        messages: conversation?.messages,
        onModeChange,
    });

    // Poll for updates when waiting for a response (not streaming locally)
    // This handles the case where user navigates away and returns mid-stream
    useEffect(() => {
        const shouldPoll = shouldPollForThinking({ messages, isStreaming });
        if (shouldPoll) {
            const pollInterval = setInterval(() => {
                refetch();
            }, 3000); // Poll every 3 seconds

            return () => clearInterval(pollInterval);
        }
        return undefined;
    }, [isStreaming, messages, refetch]);

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
                    setTitle(truncateTitle(initialMessage));
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
            setTitle(truncateTitle(inputValue));
        }
        handleSendMessage(inputValue);
        updateInputValue('');
    };

    const handleQuestionSelect = (question: string) => {
        if (isStreaming) return;
        // Update title optimistically on first message
        if (messages.length === 0 && setTitle) {
            setTitle(truncateTitle(question));
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
                    <Text size="md" weight="bold" style={{ color: colors.gray[600] }}>
                        {removeMarkdown(titleFromParent || conversation.title || 'New Chat')}
                    </Text>
                </Header>
            )}
            <ContentWrapper>
                <MessagesContainer $variant={variant}>
                    <MessagesContent $variant={variant}>
                        {showEmptyState ? (
                            <EmptyStateContainer $variant={variant}>
                                <EmptyStateLogoContainer>
                                    <ChatLogo
                                        logoUrl={
                                            appConfig.config?.visualConfig?.logoUrl || themeConfig?.assets?.logoUrl
                                        }
                                    />
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
                                        modeOptions={isModeSelectEnabled ? CHAT_MODE_OPTIONS : undefined}
                                        selectedMode={selectedMode}
                                        onModeChange={(mode) => onModeChange(mode as ChatMode)}
                                        autoFocus
                                    />
                                    <SuggestedQuestions
                                        onQuestionSelect={handleQuestionSelect}
                                        questions={suggestedQuestions}
                                    />
                                    {variant === ChatVariant.Full && <FreeTrialAIChatPopover variant="welcome" />}
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
                                    chatLocation={chatLocation}
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
                                modeOptions={isModeSelectEnabled ? CHAT_MODE_OPTIONS : undefined}
                                selectedMode={selectedMode}
                                onModeChange={(mode) => onModeChange(mode as ChatMode)}
                                autoFocus
                            />
                        </InputContent>
                    </InputContainer>
                )}
            </ContentWrapper>
            {messages.length > 1 && !isStreaming && variant === ChatVariant.Full && (
                <FreeTrialAIChatPopover variant="completion" />
            )}
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
    const [selectedMode, setSelectedMode] = useState<ChatMode>((CHAT_MODE_OPTIONS[0]?.value as ChatMode) || 'auto');
    const appConfig = useAppConfig();
    const themeConfig = useTheme();
    const isModeSelectEnabled = useIsAskDataHubModeSelectEnabled();

    // If we have a conversation, render the full chat experience
    if (conversationUrn) {
        return (
            <ChatAreaWithConversation
                conversationUrn={conversationUrn}
                variant={variant}
                suggestedQuestions={suggestedQuestions}
                welcomePlaceholder={welcomePlaceholder}
                onConversationNotFound={rest.onConversationNotFound}
                selectedMode={selectedMode}
                onModeChange={setSelectedMode}
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
                                <ChatLogo
                                    logoUrl={appConfig.config?.visualConfig?.logoUrl || themeConfig?.assets?.logoUrl}
                                />
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
                                    modeOptions={isModeSelectEnabled ? CHAT_MODE_OPTIONS : undefined}
                                    selectedMode={selectedMode}
                                    onModeChange={(mode) => setSelectedMode(mode as ChatMode)}
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
