import { Button, Loader, Text, Tooltip, colors } from '@components';
import { Chat } from '@phosphor-icons/react';
import { message as antMessage } from 'antd';
import React from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';

import { useDeleteDataHubAiConversationMutation } from '@graphql/aiChat.generated';
import { DataHubAiConversation } from '@types';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    min-height: 0;
    overflow: hidden;
`;

const Header = styled.div`
    padding: 16px;
    border-bottom: 1px solid ${colors.gray[100]};
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const ConversationsList = styled.div`
    flex: 1;
    min-height: 0;
    overflow-y: auto;
    padding: 8px;
`;

const DeleteButton = styled(Button)``;

const ConversationItem = styled.div<{ selected?: boolean }>`
    padding: 12px;
    margin-bottom: 4px;
    border-radius: 6px;
    cursor: pointer;
    display: flex;
    justify-content: space-between;
    align-items: center;
    transition: background-color 0.2s;
    background-color: ${(props) => (props.selected ? colors.primary[0] : 'transparent')};

    &:hover {
        background-color: ${(props) => (props.selected ? colors.primary[0] : colors.gray[100])};
        opacity: 0.8;
    }

    /* Hide delete button by default */
    ${DeleteButton} {
        opacity: 0;
        transition: opacity 0.2s;
    }

    /* Show delete button on hover */
    &:hover ${DeleteButton} {
        opacity: 1;
    }
`;

const ConversationContent = styled.div`
    flex: 1;
    overflow: hidden;
`;

const ConversationTitle = styled.div`
    font-weight: 500;
    font-size: 14px;
    color: #262626;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    margin-bottom: 4px;
`;

const ConversationMeta = styled.div`
    font-size: 12px;
    color: #8c8c8c;
    display: flex;
    gap: 8px;
`;

const EmptyState = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 32px 16px;
    color: #8c8c8c;
    text-align: center;
`;

const LoadingContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 32px;
`;

interface ConversationListProps {
    conversations: DataHubAiConversation[];
    selectedConversationUrn?: string;
    onSelectConversation: (conversationUrn: string) => void;
    onCreateConversation: () => void;
    onDeleteConversation: () => void;
    loading?: boolean;
    creatingConversation?: boolean;
}

export const ConversationList: React.FC<ConversationListProps> = ({
    conversations,
    selectedConversationUrn,
    onSelectConversation,
    onCreateConversation,
    onDeleteConversation,
    loading,
    creatingConversation,
}) => {
    const [deleteConversation] = useDeleteDataHubAiConversationMutation();

    const handleDelete = async (e: React.MouseEvent, conversation: DataHubAiConversation) => {
        e.stopPropagation();

        try {
            await deleteConversation({
                variables: { urn: conversation.urn },
            });

            // Emit analytics event for chat deletion
            analytics.event({
                type: EventType.DeleteDataHubChatEvent,
                conversationUrn: conversation.urn,
                messageCount: conversation.messageCount || 0,
            });

            antMessage.success('Chat deleted');
            onDeleteConversation();
        } catch (error) {
            console.error('Failed to delete chat:', error);
            antMessage.error('Failed to delete chat');
        }
    };

    const formatDate = (timestamp: number) => {
        const date = new Date(timestamp);
        const now = new Date();
        const diffInMs = now.getTime() - date.getTime();
        const diffInHours = diffInMs / (1000 * 60 * 60);

        if (diffInHours < 24) {
            return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        }
        return date.toLocaleDateString([], { month: 'short', day: 'numeric' });
    };

    // Sort conversations by lastUpdated time (most recent first)
    const sortedConversations = [...conversations].sort((a, b) => {
        return b.lastUpdated.time - a.lastUpdated.time;
    });

    return (
        <Container>
            <Header>
                <Text weight="bold" size="lg">
                    Your Chats
                </Text>
                <Tooltip title="Start a new chat" placement="bottom">
                    <Button
                        variant="text"
                        icon={{
                            icon: 'Plus',
                            source: 'phosphor',
                            size: 'lg',
                        }}
                        onClick={onCreateConversation}
                        isLoading={creatingConversation}
                        size="sm"
                        style={{ padding: 4 }}
                    />
                </Tooltip>
            </Header>
            <ConversationsList>
                {(() => {
                    // Only show loading on initial load (when there's no data yet)
                    // This prevents jitter during refetches
                    if (loading && conversations.length === 0) {
                        return (
                            <LoadingContainer>
                                <Loader size="md" />
                            </LoadingContainer>
                        );
                    }
                    if (conversations.length === 0) {
                        return (
                            <EmptyState>
                                <Chat size={48} color="gray" />
                                <Text color="gray" style={{ marginTop: '16px' }}>
                                    No conversations yet
                                </Text>
                                <Text size="sm" color="gray" style={{ marginTop: '8px' }}>
                                    Click &quot;New&quot; to start chatting
                                </Text>
                            </EmptyState>
                        );
                    }
                    return sortedConversations.map((conversation) => (
                        <ConversationItem
                            key={conversation.urn}
                            selected={conversation.urn === selectedConversationUrn}
                            onClick={() => onSelectConversation(conversation.urn)}
                        >
                            <ConversationContent>
                                <ConversationTitle>{conversation.title || 'New Chat'}</ConversationTitle>
                                <ConversationMeta>
                                    <span>{conversation.messageCount || 0} messages</span>
                                    <span>•</span>
                                    <span>{formatDate(conversation.lastUpdated.time)}</span>
                                </ConversationMeta>
                            </ConversationContent>
                            <DeleteButton
                                variant="text"
                                color="red"
                                icon={{ icon: 'Trash', source: 'phosphor', size: 'lg' }}
                                onClick={(e) => handleDelete(e, conversation)}
                            />
                        </ConversationItem>
                    ));
                })()}
            </ConversationsList>
        </Container>
    );
};
