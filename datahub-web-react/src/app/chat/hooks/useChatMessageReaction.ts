import { useCallback, useMemo, useState } from 'react';

import { ChatLocationType, ChatMessageReactionType } from '@app/analytics';
import { emitFeedbackEvent, emitReactionEvent, generateMessageId } from '@app/chat/utils/chatFeedbackUtils';

export interface UseChatMessageReactionProps {
    messageTime: number;
    messageText: string | undefined;
    conversationUrn: string | undefined;
    chatLocation: ChatLocationType | undefined;
}

export interface UseChatMessageReactionReturn {
    /** The current reaction state (null if no reaction yet) */
    reaction: ChatMessageReactionType | null;
    /** Whether the feedback modal should be shown */
    showFeedbackModal: boolean;
    /** Handle a reaction (thumbs up or down) */
    handleReaction: (newReaction: ChatMessageReactionType) => void;
    /** Handle feedback submission from the modal */
    handleFeedbackSubmit: (feedbackText: string) => void;
    /** Handle feedback modal cancellation */
    handleFeedbackCancel: () => void;
}

/**
 * Hook for managing chat message reaction state and analytics.
 * Encapsulates the logic for tracking thumbs up/down reactions and collecting feedback.
 */
export function useChatMessageReaction({
    messageTime,
    messageText,
    conversationUrn,
    chatLocation,
}: UseChatMessageReactionProps): UseChatMessageReactionReturn {
    const [reaction, setReaction] = useState<ChatMessageReactionType | null>(null);
    const [showFeedbackModal, setShowFeedbackModal] = useState(false);

    const messageId = useMemo(() => generateMessageId(messageTime, messageText), [messageTime, messageText]);

    const handleReaction = useCallback(
        (newReaction: ChatMessageReactionType) => {
            setReaction(newReaction);

            if (conversationUrn && chatLocation) {
                emitReactionEvent({
                    conversationUrn,
                    messageId,
                    reaction: newReaction,
                    chatLocation,
                });
            }

            // Show feedback modal for negative reactions
            if (newReaction === 'negative') {
                setShowFeedbackModal(true);
            }
        },
        [conversationUrn, chatLocation, messageId],
    );

    const handleFeedbackSubmit = useCallback(
        (feedbackText: string) => {
            if (conversationUrn && chatLocation) {
                emitFeedbackEvent({
                    conversationUrn,
                    messageId,
                    feedbackText,
                    chatLocation,
                });
            }
            setShowFeedbackModal(false);
        },
        [conversationUrn, chatLocation, messageId],
    );

    const handleFeedbackCancel = useCallback(() => {
        setShowFeedbackModal(false);
    }, []);

    return {
        reaction,
        showFeedbackModal,
        handleReaction,
        handleFeedbackSubmit,
        handleFeedbackCancel,
    };
}
