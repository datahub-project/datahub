import { Button, Loader, Text, colors } from '@components';
import { CaretDown, Chat, ChatsTeardrop } from '@phosphor-icons/react';
import { message as antMessage } from 'antd';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

import { getColor } from '@components/theme/utils';

import analytics, { EventType } from '@app/analytics';
import { AskDataHubIcon } from '@app/chat/components/AskDataHubIcon';
import {
    NAV_ITEM_HOVER_BOX_SHADOW,
    NAV_ITEM_HOVER_GRADIENT,
    NAV_ITEM_SELECTED_BOX_SHADOW,
    NAV_ITEM_SELECTED_GRADIENT,
} from '@app/shared/styleUtils';
import { getTimeFromNow } from '@app/shared/time/timeUtils';

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
    padding: 8px;
`;

const HeaderItem = styled.div<{ $clickable?: boolean }>`
    padding: 8px;
    height: 40px;
    border-radius: 6px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    cursor: ${(props) => (props.$clickable ? 'pointer' : 'default')};
`;

const HeaderContent = styled.div<{ $clickable?: boolean }>`
    flex: 1;
    display: flex;
    align-items: center;
    gap: 8px;
    cursor: ${(props) => (props.$clickable === false ? 'default' : 'pointer')};
`;

const HeaderTitle = styled.div`
    font-family: Mulish;
    font-weight: 600;
    font-size: 14px;
    color: ${colors.gray[600]};
    line-height: 20px;
`;

const ConversationsSection = styled.div`
    padding: 0px 8px 8px 8px;
`;

const ConversationsList = styled.div<{ $isCollapsed?: boolean }>`
    flex: ${(props) => (props.$isCollapsed ? '0' : '1')};
    min-height: 0;
    overflow-y: ${(props) => (props.$isCollapsed ? 'hidden' : 'auto')};
    padding: ${(props) => (props.$isCollapsed ? '0 8px' : '8px 8px 8px 8px')};
    max-height: ${(props) => (props.$isCollapsed ? '0' : '100%')};
    opacity: ${(props) => (props.$isCollapsed ? '0' : '1')};
    transition: all 0.3s ease;
`;

const CaretIcon = styled(CaretDown)<{ $isCollapsed?: boolean }>`
    transition: transform 0.2s ease;
    transform: ${(props) => (props.$isCollapsed ? 'rotate(-90deg)' : 'rotate(0deg)')};
`;

const DeleteButton = styled(Button)``;

const ConversationItem = styled.div<{ selected?: boolean }>`
    padding: 8px 0px 8px 8px;
    margin-bottom: 4px;
    height: 40px;
    border-radius: 6px;
    cursor: pointer;
    display: flex;
    justify-content: space-between;
    align-items: center;
    transition: all 0.2s;
    background: ${(props) => (props.selected ? NAV_ITEM_SELECTED_GRADIENT : 'transparent')};
    box-shadow: ${(props) => (props.selected ? NAV_ITEM_SELECTED_BOX_SHADOW : 'none')};

    &:hover {
        background: ${(props) => (props.selected ? NAV_ITEM_SELECTED_GRADIENT : NAV_ITEM_HOVER_GRADIENT)};
        box-shadow: ${(props) => (props.selected ? NAV_ITEM_SELECTED_BOX_SHADOW : NAV_ITEM_HOVER_BOX_SHADOW)};
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

const ActionsContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 0px;
    margin-left: 4px;
    max-width: 0;
    overflow: hidden;
    opacity: 0;
    transition:
        max-width 0.2s ease,
        opacity 0.2s ease;

    ${ConversationItem}:hover & {
        max-width: 200px;
        opacity: 1;
    }
`;

const ConversationContent = styled.div`
    flex: 1;
    min-width: 0;
    overflow: hidden;
    display: flex;
    flex-direction: column;
    gap: 2px;
`;

const ConversationTitle = styled.div<{ $isSelected?: boolean }>`
    font-family: Mulish;
    font-weight: ${(props) => (props.$isSelected ? '700' : '500')};
    font-size: 14px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    line-height: 20px;

    ${(props) =>
        props.$isSelected
            ? `
        background: linear-gradient(${getColor('primary', 300, props.theme)} 1%, ${getColor(
            'primary',
            500,
            props.theme,
        )} 99%);
        background-clip: text;
        -webkit-text-fill-color: transparent;
    `
            : `color: ${colors.gray[600]};`}
`;

const ConversationMeta = styled.div`
    font-family: Mulish;
    font-size: 12px;
    color: ${colors.gray[1800]};
    line-height: 16px;
    white-space: nowrap;
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
    const [isConversationsCollapsed, setIsConversationsCollapsed] = useState(false);

    // Sort conversations by most recent activity to show active conversations at the top.
    // This provides better UX as users typically want to continue recent conversations.
    // Backend sorts by createdAt, so we re-sort by lastUpdated.time on the frontend.
    const sortedConversations = useMemo(() => {
        return [...conversations].sort((a, b) => {
            const aTime = a.lastUpdated?.time || a.created?.time || 0;
            const bTime = b.lastUpdated?.time || b.created?.time || 0;
            return bTime - aTime;
        });
    }, [conversations]);

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

    return (
        <Container>
            <Header>
                <HeaderItem $clickable onClick={onCreateConversation}>
                    <HeaderContent>
                        <AskDataHubIcon />
                        <HeaderTitle>Ask DataHub</HeaderTitle>
                    </HeaderContent>
                    <Button
                        variant="text"
                        icon={{
                            icon: 'Plus',
                            source: 'phosphor',
                            size: 'lg',
                        }}
                        onClick={(e) => {
                            e.stopPropagation();
                            onCreateConversation();
                        }}
                        isLoading={creatingConversation}
                        size="sm"
                        style={{ padding: 4 }}
                    />
                </HeaderItem>
            </Header>
            <ConversationsSection>
                <HeaderItem $clickable onClick={() => setIsConversationsCollapsed(!isConversationsCollapsed)}>
                    <HeaderContent $clickable={false}>
                        <ChatsTeardrop size={20} weight="regular" color={colors.gray[1800]} />
                        <HeaderTitle>Recents</HeaderTitle>
                    </HeaderContent>
                    <Button
                        variant="text"
                        onClick={(e) => {
                            e.stopPropagation();
                            setIsConversationsCollapsed(!isConversationsCollapsed);
                        }}
                        size="sm"
                        style={{ padding: 4 }}
                    >
                        <CaretIcon size={16} $isCollapsed={isConversationsCollapsed} color={colors.gray[1800]} />
                    </Button>
                </HeaderItem>
            </ConversationsSection>
            <ConversationsList $isCollapsed={isConversationsCollapsed}>
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
                    return sortedConversations.map((conversation) => {
                        const isSelected = conversation.urn === selectedConversationUrn;
                        return (
                            <ConversationItem
                                key={conversation.urn}
                                selected={isSelected}
                                onClick={() => onSelectConversation(conversation.urn)}
                            >
                                <ConversationContent>
                                    <ConversationTitle $isSelected={isSelected}>
                                        {conversation.title || 'New Chat'}
                                    </ConversationTitle>
                                </ConversationContent>
                                <ActionsContainer>
                                    <ConversationMeta>
                                        {getTimeFromNow(conversation.lastUpdated?.time || conversation.created?.time)}
                                    </ConversationMeta>
                                    <DeleteButton
                                        variant="text"
                                        color="red"
                                        icon={{ icon: 'Trash', source: 'phosphor', size: 'lg' }}
                                        onClick={(e) => handleDelete(e, conversation)}
                                    />
                                </ActionsContainer>
                            </ConversationItem>
                        );
                    });
                })()}
            </ConversationsList>
        </Container>
    );
};
