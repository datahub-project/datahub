import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ReactionButtons } from '@app/chat/components/messages/ReactionButtons';

// Mock the feature flag hook - default to enabled
vi.mock('@app/useAppConfig', () => ({
    useIsAiChatFeedbackEnabled: vi.fn(() => true),
}));

// Mock the components library
vi.mock('@components', () => ({
    Button: ({ onClick, icon, style, 'data-testid': testId, disabled, ...props }: any) => (
        <button
            type="button"
            onClick={onClick}
            style={style}
            data-testid={testId}
            disabled={disabled}
            data-icon-weight={icon?.weight}
            {...props}
        >
            {icon?.icon}
        </button>
    ),
    colors: {
        primary: {
            500: '#7c3aed',
        },
    },
}));

describe('ReactionButtons', () => {
    const defaultProps = {
        reaction: null as 'positive' | 'negative' | null,
        onReaction: vi.fn(),
    };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should render both thumbs up and thumbs down buttons by default', () => {
        render(<ReactionButtons {...defaultProps} />);

        expect(screen.getByTestId('reaction-thumbs-up')).toBeInTheDocument();
        expect(screen.getByTestId('reaction-thumbs-down')).toBeInTheDocument();
    });

    it('should only render thumbs up when showThumbsDown is false', () => {
        render(<ReactionButtons {...defaultProps} showThumbsDown={false} />);

        expect(screen.getByTestId('reaction-thumbs-up')).toBeInTheDocument();
        expect(screen.queryByTestId('reaction-thumbs-down')).not.toBeInTheDocument();
    });

    it('should only render thumbs down when showThumbsUp is false', () => {
        render(<ReactionButtons {...defaultProps} showThumbsUp={false} />);

        expect(screen.queryByTestId('reaction-thumbs-up')).not.toBeInTheDocument();
        expect(screen.getByTestId('reaction-thumbs-down')).toBeInTheDocument();
    });

    it('should return null when both buttons are hidden', () => {
        const { container } = render(<ReactionButtons {...defaultProps} showThumbsUp={false} showThumbsDown={false} />);

        expect(container.firstChild).toBeNull();
    });

    it('should call onReaction with positive when thumbs up is clicked', () => {
        const onReaction = vi.fn();
        render(<ReactionButtons {...defaultProps} onReaction={onReaction} />);

        fireEvent.click(screen.getByTestId('reaction-thumbs-up'));

        expect(onReaction).toHaveBeenCalledWith('positive');
    });

    it('should call onReaction with negative when thumbs down is clicked', () => {
        const onReaction = vi.fn();
        render(<ReactionButtons {...defaultProps} onReaction={onReaction} />);

        fireEvent.click(screen.getByTestId('reaction-thumbs-down'));

        expect(onReaction).toHaveBeenCalledWith('negative');
    });

    it('should be visible when reaction is set', () => {
        const { container } = render(<ReactionButtons {...defaultProps} reaction="positive" />);

        const wrapper = container.firstChild as HTMLElement;
        expect(wrapper).toHaveStyle('opacity: 1');
        expect(wrapper).toHaveStyle('visibility: visible');
    });

    it('should be visible when alwaysVisible is true', () => {
        const { container } = render(<ReactionButtons {...defaultProps} alwaysVisible />);

        const wrapper = container.firstChild as HTMLElement;
        expect(wrapper).toHaveStyle('opacity: 1');
        expect(wrapper).toHaveStyle('visibility: visible');
    });

    it('should disable both buttons when a reaction is set', () => {
        render(<ReactionButtons {...defaultProps} reaction="positive" />);

        expect(screen.getByTestId('reaction-thumbs-up')).toBeDisabled();
        expect(screen.getByTestId('reaction-thumbs-down')).toBeDisabled();
    });

    it('should use filled icon weight for selected positive reaction', () => {
        render(<ReactionButtons {...defaultProps} reaction="positive" />);

        const thumbsUp = screen.getByTestId('reaction-thumbs-up');
        const thumbsDown = screen.getByTestId('reaction-thumbs-down');

        expect(thumbsUp).toHaveAttribute('data-icon-weight', 'fill');
        expect(thumbsDown).not.toHaveAttribute('data-icon-weight', 'fill');
    });

    it('should use filled icon weight for selected negative reaction', () => {
        render(<ReactionButtons {...defaultProps} reaction="negative" />);

        const thumbsUp = screen.getByTestId('reaction-thumbs-up');
        const thumbsDown = screen.getByTestId('reaction-thumbs-down');

        expect(thumbsUp).not.toHaveAttribute('data-icon-weight', 'fill');
        expect(thumbsDown).toHaveAttribute('data-icon-weight', 'fill');
    });

    it('should not disable buttons when no reaction is set', () => {
        render(<ReactionButtons {...defaultProps} />);

        expect(screen.getByTestId('reaction-thumbs-up')).not.toBeDisabled();
        expect(screen.getByTestId('reaction-thumbs-down')).not.toBeDisabled();
    });

    it('should return null when feature flag is disabled', async () => {
        const { useIsAiChatFeedbackEnabled } = await import('@app/useAppConfig');
        vi.mocked(useIsAiChatFeedbackEnabled).mockReturnValue(false);

        const { container } = render(<ReactionButtons {...defaultProps} />);

        expect(container.firstChild).toBeNull();

        // Reset mock for other tests
        vi.mocked(useIsAiChatFeedbackEnabled).mockReturnValue(true);
    });
});
