import React from 'react';

import { ChatMessage } from '@app/chat/components/messages/ChatMessage';
import { ThinkingGroup } from '@app/chat/components/messages/ThinkingGroup';
import { ChatMessageAction, ChatVariant } from '@app/chat/types';
import { MessageGroup } from '@app/chat/utils/messageGrouping';

import { Entity } from '@types';

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
                const isLast = index === messageGroups.length - 1;
                const isThinking = group.type === 'thinking';
                let compactSpacing = 0;
                if (variant === ChatVariant.Compact) {
                    if (isLast) {
                        compactSpacing = 0;
                    } else if (isThinking) {
                        compactSpacing = 4;
                    } else {
                        compactSpacing = 32;
                    }
                }
                if (isThinking) {
                    const firstMessageTime = group.messages[0]?.time || index;
                    // Thinking group is complete if there's a next group (non-thinking) or if we're not streaming
                    const isComplete = index < messageGroups.length - 1 || !isStreaming;
                    return (
                        <div key={`thinking-group-${firstMessageTime}`} style={{ marginBottom: compactSpacing }}>
                            <ThinkingGroup
                                messages={group.messages}
                                verboseMode={verboseMode}
                                isComplete={isComplete}
                            />
                        </div>
                    );
                }
                const messageText = group.message.content?.text || '';
                const messageKey = `${group.message.time}-${messageText.substring(0, 20)}`;

                return (
                    <div key={messageKey} style={{ marginBottom: compactSpacing }}>
                        <ChatMessage
                            message={group.message}
                            variant={variant}
                            allowedActions={messageActions}
                            showReferences={showReferences}
                            conversationUrn={conversationUrn}
                            selectedEntityUrn={selectedEntityUrn}
                            onEntitySelect={onEntitySelect}
                        />
                    </div>
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
