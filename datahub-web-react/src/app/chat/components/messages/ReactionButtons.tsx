import { Button, colors } from '@components';
import React from 'react';
import styled from 'styled-components';

import { ChatMessageReactionType } from '@app/analytics';
import { useIsAiChatFeedbackEnabled } from '@app/useAppConfig';

const Container = styled.div<{ $alwaysVisible?: boolean }>`
    display: flex;
    gap: 4px;
    ${(props) =>
        !props.$alwaysVisible &&
        `
        opacity: 0;
        visibility: hidden;
        transition:
            opacity 0.2s ease,
            visibility 0.2s ease;
    `}
`;

export interface ReactionButtonsProps {
    /** Current reaction state */
    reaction: ChatMessageReactionType | null;
    /** Whether to show thumbs up button */
    showThumbsUp?: boolean;
    /** Whether to show thumbs down button */
    showThumbsDown?: boolean;
    /** Callback when a reaction is selected */
    onReaction: (reaction: ChatMessageReactionType) => void;
    /** Whether buttons should always be visible (vs hover-to-show) */
    alwaysVisible?: boolean;
    /** Additional className for styling */
    className?: string;
}

/**
 * Reusable component for thumbs up/down reaction buttons.
 * Handles visual state (highlighting selected reaction) and callbacks.
 */
export const ReactionButtons: React.FC<ReactionButtonsProps> = ({
    reaction,
    showThumbsUp = true,
    showThumbsDown = true,
    onReaction,
    alwaysVisible = false,
    className,
}) => {
    const isFeedbackEnabled = useIsAiChatFeedbackEnabled();

    // If reaction is set, force visibility
    const isVisible = alwaysVisible || reaction !== null;
    // Disable buttons after a reaction has been provided
    const isDisabled = reaction !== null;

    // Hide if feature flag is disabled or no buttons to show
    if (!isFeedbackEnabled || (!showThumbsUp && !showThumbsDown)) {
        return null;
    }

    const isPositiveSelected = reaction === 'positive';
    const isNegativeSelected = reaction === 'negative';

    return (
        <Container
            $alwaysVisible={isVisible}
            className={className}
            style={isVisible ? { opacity: 1, visibility: 'visible' } : undefined}
        >
            {showThumbsUp && (
                <Button
                    variant="text"
                    size="md"
                    color="gray"
                    onClick={() => onReaction('positive')}
                    disabled={isDisabled}
                    icon={{
                        icon: 'ThumbsUp',
                        source: 'phosphor',
                        size: 'md',
                        weight: isPositiveSelected ? 'fill' : undefined,
                    }}
                    style={{
                        padding: '4px 8px',
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
                    icon={{
                        icon: 'ThumbsDown',
                        source: 'phosphor',
                        size: 'md',
                        weight: isNegativeSelected ? 'fill' : undefined,
                    }}
                    style={{
                        padding: '4px 8px',
                        minWidth: 'auto',
                        color: isNegativeSelected ? colors.primary[500] : undefined,
                    }}
                    data-testid="reaction-thumbs-down"
                />
            )}
        </Container>
    );
};
