import { useEffect, useRef } from 'react';

import { ChatMode, getModeFromLastAgentMessage } from '@app/chat/utils/chatModes';

/**
 * Message type that may include agentName.
 * Uses a permissive type to handle cases where TypeScript types
 * haven't been regenerated to include agentName yet.
 */
type MessageWithOptionalAgentName = Record<string, unknown> & {
    agentName?: string | null;
};

interface UseSyncChatModeProps {
    /** Current conversation URN */
    conversationUrn: string;
    /** Messages from the conversation (may be undefined while loading) */
    messages: MessageWithOptionalAgentName[] | undefined;
    /** Callback to update the selected mode */
    onModeChange: (mode: ChatMode) => void;
}

/**
 * Hook to sync the chat mode from conversation messages.
 *
 * Syncs the mode once when:
 * - A new conversation is loaded (different URN)
 * - Messages become available for the current conversation
 *
 * This ensures the mode is correctly set even when data loads asynchronously.
 */
export const useSyncChatMode = ({ conversationUrn, messages, onModeChange }: UseSyncChatModeProps): void => {
    const prevConversationUrnRef = useRef<string | null>(null);
    const hasSyncedModeRef = useRef<boolean>(false);

    useEffect(() => {
        const isNewConversation = conversationUrn !== prevConversationUrnRef.current;

        if (isNewConversation) {
            prevConversationUrnRef.current = conversationUrn;
            hasSyncedModeRef.current = false;
        }

        // Update mode if we have messages and haven't synced yet for this conversation
        if (messages && !hasSyncedModeRef.current) {
            onModeChange(getModeFromLastAgentMessage(messages));
            hasSyncedModeRef.current = true;
        }
    }, [conversationUrn, messages, onModeChange]);
};
