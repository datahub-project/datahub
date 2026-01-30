import { Button, colors } from '@components';
import React from 'react';

import { ChatMessageReactionType } from '@app/analytics';
import { useIsAiChatFeedbackEnabled } from '@app/useAppConfig';

export interface ReactionButtonsProps {
    /** Current reaction state */
    reaction: ChatMessageReactionType | null;
    /** Whether to show thumbs up button */
    showThumbsUp?: boolean;
    /** Whether to show thumbs down button */
    showThumbsDown?: boolean;
    /** Callback when a reaction is selected */
    onReaction: (reaction: ChatMessageReactionType) => void;
}

/**
 * Reusable component for thumbs up/down reaction buttons.
 * Handles visual state (highlighting selected reaction) and callbacks.
 * Returns buttons directly without a wrapper - parent container controls layout.
 */
export const ReactionButtons: React.FC<ReactionButtonsProps> = ({
    reaction,
    showThumbsUp = true,
    showThumbsDown = true,
    onReaction,
}) => {
    const isFeedbackEnabled = useIsAiChatFeedbackEnabled();

    // Disable buttons after a reaction has been provided
    const isDisabled = reaction !== null;

    // Hide if feature flag is disabled or no buttons to show
    if (!isFeedbackEnabled || (!showThumbsUp && !showThumbsDown)) {
        return null;
    }

    const isPositiveSelected = reaction === 'positive';
    const isNegativeSelected = reaction === 'negative';

    return (
        <>
            {showThumbsUp && (
                <Button
                    variant="text"
                    size="md"
                    color="gray"
                    onClick={() => onReaction('positive')}
                    disabled={isDisabled}
                    aria-label="Helpful response"
                    aria-pressed={isPositiveSelected}
                    icon={{
                        icon: 'ThumbsUp',
                        source: 'phosphor',
                        size: 'lg',
                        weight: isPositiveSelected ? 'fill' : undefined,
                    }}
                    style={{
                        padding: '4px',
                        minWidth: 'auto',
                        color: isPositiveSelected ? colors.primary[500] : undefined,
                    }}
                    data-testid="reaction-thumbs-up"
                />
            )}
            {showThumbsDown && (
                <Button
                    variant="text"
                    size="md"
                    color="gray"
                    onClick={() => onReaction('negative')}
                    disabled={isDisabled}
                    aria-label="Unhelpful response"
                    aria-pressed={isNegativeSelected}
                    icon={{
                        icon: 'ThumbsDown',
                        source: 'phosphor',
                        size: 'lg',
                        weight: isNegativeSelected ? 'fill' : undefined,
                    }}
                    style={{
                        padding: '4px',
                        minWidth: 'auto',
                        color: isNegativeSelected ? colors.primary[500] : undefined,
                    }}
                    data-testid="reaction-thumbs-down"
                />
            )}
        </>
    );
};
