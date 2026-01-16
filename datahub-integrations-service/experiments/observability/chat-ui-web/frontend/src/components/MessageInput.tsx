import { useState, KeyboardEvent, forwardRef, useImperativeHandle, useRef } from 'react';

interface MessageInputProps {
  onSend: (message: string) => void;
  disabled: boolean;
  placeholder?: string;
}

export interface MessageInputHandle {
  focus: () => void;
}

export const MessageInput = forwardRef<MessageInputHandle, MessageInputProps>(
  ({ onSend, disabled, placeholder = 'Type your message...' }, ref) => {
    const [input, setInput] = useState('');
    const textareaRef = useRef<HTMLTextAreaElement>(null);

    // Expose focus method to parent component
    useImperativeHandle(ref, () => ({
      focus: () => {
        textareaRef.current?.focus();
      },
    }));

    const handleSend = () => {
      if (input.trim() && !disabled) {
        onSend(input.trim());
        setInput('');
      }
    };

    const handleKeyDown = (e: KeyboardEvent<HTMLTextAreaElement>) => {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        handleSend();
      }
    };

    return (
      <div className="message-input">
        <textarea
          ref={textareaRef}
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder={placeholder}
          disabled={disabled}
          rows={3}
        />
        <button onClick={handleSend} disabled={disabled || !input.trim()}>
          Send
        </button>
      </div>
    );
  }
);
