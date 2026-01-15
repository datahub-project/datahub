import React from 'react';

import { ChatMessage } from '@app/chat/components/messages/ChatMessage';
import { ThinkingGroup } from '@app/chat/components/messages/ThinkingGroup';
import { ChatMessageAction, ChatVariant } from '@app/chat/types';
import { MessageGroup } from '@app/chat/utils/messageGrouping';
import { THINKING_WINDOW_MS, isUserOrAgentThinkingRecent } from '@app/chat/utils/thinkingState';

import { DataHubAiConversationMessage, Entity } from '@types';

const isThinkingGroupComplete = ({
    messages,
    isStreaming,
    isLastGroup,
}: {
    messages: DataHubAiConversationMessage[];
    isStreaming: boolean;
    isLastGroup: boolean;
}): boolean => {
    if (isStreaming) return false;
    if (!isLastGroup) return true;
    return !isUserOrAgentThinkingRecent(messages, Date.now(), THINKING_WINDOW_MS);
};

interface MessageListProps {
    messageGroups: MessageGroup[];
    verboseMode: boolean;
    isStreaming: boolean;
    variant?: ChatVariant;
    messageActions?: ChatMessageAction[];
    showReferences?: boolean;
    conversationUrn?: string;
    selectedEntityUrn?: string;
    onEntitySelect?: (entity: Entity | null) => void;
}

/**
 * Shared component for rendering message groups (regular messages and thinking groups)
 * Used by both ChatArea and EmbeddedChat
 */
export const MessageList: React.FC<MessageListProps> = ({
    messageGroups,
    verboseMode,
    isStreaming,
    variant = ChatVariant.Full,
    messageActions,
    showReferences = true,
    conversationUrn,
    selectedEntityUrn,
    onEntitySelect,
}) => {
    return (
        <>
            {messageGroups.map((group, index) => {
                const isThinking = group.type === 'thinking';
                if (isThinking) {
                    const firstMessageTime = group.messages[0]?.time || index;
                    const isLastGroup = index === messageGroups.length - 1;
                    const isComplete = isThinkingGroupComplete({
                        messages: group.messages,
                        isStreaming,
                        isLastGroup,
                    });

                    return (
                        <ThinkingGroup
                            key={`thinking-group-${firstMessageTime}`}
                            messages={group.messages}
                            verboseMode={verboseMode}
                            isComplete={isComplete}
                        />
                    );
                }
                const messageText = group.message.content?.text || '';
                const messageKey = `${group.message.time}-${messageText.substring(0, 20)}`;

                return (
                    <ChatMessage
                        key={messageKey}
                        message={group.message}
                        variant={variant}
                        allowedActions={messageActions}
                        showReferences={showReferences}
                        conversationUrn={conversationUrn}
                        selectedEntityUrn={selectedEntityUrn}
                        onEntitySelect={onEntitySelect}
                    />
                );
            })}
            {isStreaming &&
                (messageGroups.length === 0 || messageGroups[messageGroups.length - 1].type !== 'thinking') && (
                    <ThinkingGroup
                        key="streaming-thinking"
                        messages={[]}
                        verboseMode={verboseMode}
                        isComplete={false}
                    />
                )}
        </>
    );
};
