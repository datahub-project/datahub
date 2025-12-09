import React from 'react';

import { ChatMessage } from '@app/chat/components/messages/ChatMessage';
import { ThinkingGroup } from '@app/chat/components/messages/ThinkingGroup';
import { MessageGroup } from '@app/chat/utils/messageGrouping';

import { Entity } from '@types';

interface MessageListProps {
    messageGroups: MessageGroup[];
    verboseMode: boolean;
    isStreaming: boolean;
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
    selectedEntityUrn,
    onEntitySelect,
}) => {
    return (
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
                            verboseMode={verboseMode}
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
