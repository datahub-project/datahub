import { colors } from '@components';
import { message as antMessage } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { MessageList } from '@app/chat/components/MessageList';
import { ChatInput } from '@app/chat/components/input/ChatInput';
import { useChatMessages } from '@app/chat/hooks/useChatMessages';
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
    gap: 16px;
`;

const InputContainer = styled.div`
    padding: 16px;
    border-top: 1px solid ${colors.gray[100]};
    background-color: #ffffff;
`;

const EmptyState = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    flex: 1;
    color: ${colors.gray[400]};
    font-size: 14px;
`;

interface EmbeddedChatProps {
    context: string;
    originType: DataHubAiConversationOriginType;
    title?: string;
}

/**
 * Simplified chat panel for embedded use in sidebars and panels.
 * - No welcome screen or suggestions
 * - Just messages + input at bottom
 * - Takes context as a prop to pass to conversation creation
 */
export const EmbeddedChat: React.FC<EmbeddedChatProps> = ({ context, originType, title }) => {
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
            agentName: 'IngestionTroubleshooter',
        });

    const handleSend = async () => {
        if (!inputValue.trim()) {
            return;
        }

        const messageText = inputValue;
        setInputValue('');

        // If conversation doesn't exist yet, create it first
        if (!conversationUrn) {
            try {
                const result = await createConversation({
                    variables: {
                        input: {
                            title: title || null,
                            originType,
                            context: {
                                text: context,
                            },
                        },
                    },
                });

                if (result.data?.createDataHubAiConversation) {
                    const newConversationUrn = result.data.createDataHubAiConversation.urn;
                    setConversationUrn(newConversationUrn);
                    handleSendMessage(messageText, newConversationUrn);
                } else {
                    throw new Error('No conversation URN returned');
                }
            } catch (error) {
                console.error('Failed to create conversation:', error);
                antMessage.error('Failed to create chat conversation');
                setInputValue(messageText);
            }
        } else {
            // Conversation already exists, use the hook's handler
            handleSendMessage(messageText);
        }
    };

    return (
        <Container>
            <MessagesContainer>
                {messages.length === 0 ? (
                    <EmptyState>Start a conversation by typing a message below</EmptyState>
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
