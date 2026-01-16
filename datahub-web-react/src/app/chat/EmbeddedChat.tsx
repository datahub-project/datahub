import { Pill, colors } from '@components';
import { message as antMessage } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { ChatLocationType } from '@app/analytics';
import { AskDataHubIcon } from '@app/chat/components/AskDataHubIcon';
import { MessageList } from '@app/chat/components/MessageList';
import { SuggestedQuestions } from '@app/chat/components/SuggestedQuestions';
import { ChatInput } from '@app/chat/components/input/ChatInput';
import { useChatMessages } from '@app/chat/hooks/useChatMessages';
import { MessageContext } from '@app/chat/hooks/useChatStream';
import { ChatFeatureFlags } from '@app/chat/types';
import { useGetAuthenticatedUserUrn } from '@app/useGetAuthenticatedUser';

import { useCreateDataHubAiConversationMutation } from '@graphql/aiChat.generated';
import { DataHubAiConversationOriginType } from '@types';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    height: 100%;
    background-color: #ffffff;
    overflow: hidden;
`;

const MessagesContainer = styled.div`
    flex: 1;
    min-height: 0;
    overflow-y: auto;
    padding: 16px;
    display: flex;
    flex-direction: column;
    gap: 32px;
`;

const InputContainer = styled.div`
    padding: 16px;
    background-color: #ffffff;
`;

const EmptyState = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    flex: 1;
    color: ${colors.gray[400]};
    font-size: 14px;
    flex-direction: column;
    gap: 12px;
`;

interface EmbeddedChatProps {
    context?: string;
    agentName?: string;
    originType: DataHubAiConversationOriginType;
    title?: string;
    contentPlaceholder?: string;
    getMessageContext?: () => MessageContext;
    chatLocation: ChatLocationType;
    suggestedQuestions?: string[];
}

/**
 * Simplified chat panel for embedded use in sidebars and panels.
 * - No welcome screen or suggestions
 * - Just messages + input at bottom
 * - Takes context as a prop to pass to conversation creation (conversation-level context)
 * - Optionally takes getMessageContext callback to provide dynamic context with each message (message-level context)
 */
export const EmbeddedChat: React.FC<EmbeddedChatProps> = ({
    context,
    agentName,
    originType,
    title,
    contentPlaceholder,
    getMessageContext,
    chatLocation,
    suggestedQuestions,
}) => {
    const userUrn = useGetAuthenticatedUserUrn();
    const [conversationUrn, setConversationUrn] = useState<string | null>(null);
    const [inputValue, setInputValue] = useState('');
    const [featureFlags] = useState<ChatFeatureFlags>({
        verboseMode: false,
    });
    const [createConversation] = useCreateDataHubAiConversationMutation();

    // Use shared chat messages hook
    const { messages, messageGroups, isStreaming, handleSendMessage, handleStopStreaming, messagesEndRef } =
        useChatMessages({
            conversationUrn: conversationUrn || '',
            userUrn,
            agentName,
            chatLocation,
        });

    const sendMessage = async (messageText: string) => {
        // Get message context if callback provided
        const messageContext = getMessageContext ? getMessageContext() : undefined;

        // If conversation doesn't exist yet, create it first
        if (!conversationUrn) {
            try {
                const result = await createConversation({
                    variables: {
                        input: {
                            title: title || null,
                            originType,
                            context: context
                                ? {
                                      text: context,
                                  }
                                : undefined,
                        },
                    },
                });

                if (result.data?.createDataHubAiConversation) {
                    const newConversationUrn = result.data.createDataHubAiConversation.urn;
                    setConversationUrn(newConversationUrn);
                    handleSendMessage(messageText, newConversationUrn, messageContext);
                } else {
                    throw new Error('No conversation URN returned');
                }
            } catch (error) {
                console.error('Failed to create conversation:', error);
                antMessage.error('Failed to create chat conversation');
                setInputValue(messageText);
            }
        } else {
            // Conversation already exists, use the hook's handler with message context
            handleSendMessage(messageText, undefined, messageContext);
        }
    };

    const handleSend = async () => {
        if (!inputValue.trim()) {
            return;
        }

        const messageText = inputValue;
        setInputValue('');
        await sendMessage(messageText);
    };

    const handleQuestionSelect = async (question: string) => {
        await sendMessage(question);
    };

    return (
        <Container>
            <MessagesContainer>
                {messages.length === 0 ? (
                    <>
                        {contentPlaceholder && (
                            <EmptyState>
                                <div>
                                    <AskDataHubIcon size={28} />
                                </div>
                                <div>{contentPlaceholder}</div>
                                <Pill size="sm" color="blue" label="BETA" clickable={false} />
                            </EmptyState>
                        )}
                        {suggestedQuestions && suggestedQuestions.length > 0 && (
                            <SuggestedQuestions
                                onQuestionSelect={handleQuestionSelect}
                                questions={suggestedQuestions}
                            />
                        )}
                    </>
                ) : (
                    <>
                        <MessageList
                            messageGroups={messageGroups}
                            verboseMode={featureFlags.verboseMode}
                            isStreaming={isStreaming}
                        />
                        <div ref={messagesEndRef} />
                    </>
                )}
            </MessagesContainer>
            <InputContainer>
                <ChatInput
                    value={inputValue}
                    onChange={setInputValue}
                    onSubmit={handleSend}
                    onStop={handleStopStreaming}
                    placeholder="Ask about this run..."
                    isStreaming={isStreaming}
                />
            </InputContainer>
        </Container>
    );
};
