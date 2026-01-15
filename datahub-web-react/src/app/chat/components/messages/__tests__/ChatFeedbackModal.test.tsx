import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ChatFeedbackModal } from '@app/chat/components/messages/ChatFeedbackModal';

// Mock the components library
vi.mock('@components', () => ({
    Modal: ({ children, title, buttons, onCancel, dataTestId }: any) => (
        <div data-testid={dataTestId}>
            <h1>{title}</h1>
            <div>{children}</div>
            <div>
                {buttons?.map((btn: any, i: number) => (
                    <button
                        key={btn.text}
                        type="button"
                        onClick={btn.onClick}
                        disabled={btn.disabled}
                        data-testid={`modal-btn-${i}`}
                    >
                        {btn.text}
                    </button>
                ))}
            </div>
            <button type="button" onClick={onCancel} data-testid="modal-close">
                Close
            </button>
        </div>
    ),
    TextArea: ({ placeholder, value, onChange, rows }: any) => (
        <textarea placeholder={placeholder} value={value} onChange={onChange} rows={rows} data-testid="textarea" />
    ),
}));

describe('ChatFeedbackModal', () => {
    const defaultProps = {
        onSubmit: vi.fn(),
        onCancel: vi.fn(),
    };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should render the modal with title and placeholder', () => {
        render(<ChatFeedbackModal {...defaultProps} />);

        expect(screen.getByText('Provide Feedback')).toBeInTheDocument();
        expect(screen.getByPlaceholderText('What can we do better?')).toBeInTheDocument();
    });

    it('should render Cancel and Submit buttons', () => {
        render(<ChatFeedbackModal {...defaultProps} />);

        expect(screen.getByText('Cancel')).toBeInTheDocument();
        expect(screen.getByText('Submit')).toBeInTheDocument();
    });

    it('should call onCancel when Cancel button is clicked', () => {
        render(<ChatFeedbackModal {...defaultProps} />);

        fireEvent.click(screen.getByText('Cancel'));

        expect(defaultProps.onCancel).toHaveBeenCalledTimes(1);
    });

    it('should have Submit button disabled when textarea is empty', () => {
        render(<ChatFeedbackModal {...defaultProps} />);

        const submitButton = screen.getByText('Submit');
        expect(submitButton).toBeDisabled();
    });

    it('should enable Submit button when text is entered', () => {
        render(<ChatFeedbackModal {...defaultProps} />);

        const textarea = screen.getByPlaceholderText('What can we do better?');
        fireEvent.change(textarea, { target: { value: 'Some feedback' } });

        const submitButton = screen.getByText('Submit');
        expect(submitButton).not.toBeDisabled();
    });

    it('should call onSubmit with feedback text when Submit is clicked', () => {
        render(<ChatFeedbackModal {...defaultProps} />);

        const textarea = screen.getByPlaceholderText('What can we do better?');
        fireEvent.change(textarea, { target: { value: 'The response was not accurate' } });

        fireEvent.click(screen.getByText('Submit'));

        expect(defaultProps.onSubmit).toHaveBeenCalledWith('The response was not accurate');
    });

    it('should keep Submit disabled when only whitespace is entered', () => {
        render(<ChatFeedbackModal {...defaultProps} />);

        const textarea = screen.getByPlaceholderText('What can we do better?');
        fireEvent.change(textarea, { target: { value: '   ' } });

        const submitButton = screen.getByText('Submit');
        expect(submitButton).toBeDisabled();
    });

    it('should have correct data-testid', () => {
        render(<ChatFeedbackModal {...defaultProps} />);

        expect(screen.getByTestId('chat-feedback-modal')).toBeInTheDocument();
    });
});
