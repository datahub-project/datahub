import { Modal, TextArea } from '@components';
import React, { useState } from 'react';

interface ChatFeedbackModalProps {
    onSubmit: (feedback: string) => void;
    onCancel: () => void;
}

export const ChatFeedbackModal: React.FC<ChatFeedbackModalProps> = ({ onSubmit, onCancel }) => {
    const [feedback, setFeedback] = useState('');

    const handleSubmit = () => {
        onSubmit(feedback);
    };

    return (
        <Modal
            title="Provide Feedback"
            onCancel={onCancel}
            buttons={[
                {
                    text: 'Cancel',
                    variant: 'outline',
                    onClick: onCancel,
                },
                {
                    text: 'Submit',
                    variant: 'filled',
                    onClick: handleSubmit,
                    disabled: !feedback.trim(),
                },
            ]}
            dataTestId="chat-feedback-modal"
        >
            <TextArea
                placeholder="What can we do better?"
                value={feedback}
                onChange={(e) => setFeedback(e.target.value)}
                rows={4}
            />
        </Modal>
    );
};
