import React, { useState, useRef, useEffect } from 'react';
import styled, { keyframes } from 'styled-components';

// ─── Animations ────────────────────────────────────────────────────────────────

const slideIn = keyframes`
    from { opacity: 0; transform: translateY(20px) scale(0.95); }
    to   { opacity: 1; transform: translateY(0) scale(1); }
`;

// ─── Floating Button ────────────────────────────────────────────────────────────

const FloatingButton = styled.button<{ $isOpen: boolean }>`
    position: fixed;
    bottom: 28px;
    right: 28px;
    z-index: 9999;
    width: 52px;
    height: 52px;
    border-radius: 50%;
    border: none;
    background: ${({ $isOpen }) => ($isOpen ? '#5c4fcf' : '#7c6af7')};
    color: white;
    font-size: 22px;
    cursor: pointer;
    box-shadow: 0 4px 16px rgba(92, 79, 207, 0.45);
    display: flex;
    align-items: center;
    justify-content: center;
    transition: background 0.2s, transform 0.2s, box-shadow 0.2s;

    &:hover {
        background: #5c4fcf;
        transform: scale(1.08);
        box-shadow: 0 6px 20px rgba(92, 79, 207, 0.55);
    }
`;

// ─── Chat Panel ─────────────────────────────────────────────────────────────────

const ChatPanel = styled.div`
    position: fixed;
    bottom: 92px;
    right: 28px;
    z-index: 9998;
    width: 380px;
    height: 520px;
    background: #ffffff;
    border-radius: 16px;
    box-shadow: 0 8px 40px rgba(0, 0, 0, 0.18);
    display: flex;
    flex-direction: column;
    overflow: hidden;
    animation: ${slideIn} 0.22s ease;
`;

const PanelHeader = styled.div`
    background: linear-gradient(135deg, #7c6af7 0%, #5c4fcf 100%);
    padding: 16px 18px;
    color: white;
    display: flex;
    align-items: center;
    justify-content: space-between;
    flex-shrink: 0;
`;

const HeaderTitle = styled.div`
    display: flex;
    align-items: center;
    gap: 10px;
    font-weight: 600;
    font-size: 15px;
`;

const HeaderSubtitle = styled.div`
    font-size: 11px;
    opacity: 0.8;
    margin-top: 2px;
`;

const CloseBtn = styled.button`
    background: none;
    border: none;
    color: white;
    font-size: 18px;
    cursor: pointer;
    padding: 2px 6px;
    border-radius: 4px;
    opacity: 0.8;
    &:hover { opacity: 1; background: rgba(255,255,255,0.15); }
`;

const MessagesArea = styled.div`
    flex: 1;
    overflow-y: auto;
    padding: 16px;
    display: flex;
    flex-direction: column;
    gap: 12px;
    background: #f8f7ff;
`;

const Message = styled.div<{ $isUser: boolean }>`
    max-width: 85%;
    padding: 10px 14px;
    border-radius: ${({ $isUser }) => ($isUser ? '16px 16px 4px 16px' : '16px 16px 16px 4px')};
    background: ${({ $isUser }) => ($isUser ? '#7c6af7' : '#ffffff')};
    color: ${({ $isUser }) => ($isUser ? 'white' : '#1a1a2e')};
    font-size: 13.5px;
    line-height: 1.5;
    align-self: ${({ $isUser }) => ($isUser ? 'flex-end' : 'flex-start')};
    box-shadow: 0 1px 4px rgba(0,0,0,0.08);
`;

const TypingIndicator = styled.div`
    align-self: flex-start;
    background: white;
    border-radius: 16px 16px 16px 4px;
    padding: 10px 16px;
    font-size: 20px;
    letter-spacing: 2px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.08);
`;

const InputArea = styled.div`
    padding: 12px 14px;
    border-top: 1px solid #ece9ff;
    display: flex;
    gap: 8px;
    background: white;
    flex-shrink: 0;
`;

const Input = styled.input`
    flex: 1;
    border: 1.5px solid #ddd6ff;
    border-radius: 22px;
    padding: 9px 16px;
    font-size: 13.5px;
    outline: none;
    background: #faf9ff;
    color: #1a1a2e;

    &:focus {
        border-color: #7c6af7;
        background: white;
    }

    &::placeholder { color: #aaa; }
`;

const SendBtn = styled.button`
    width: 38px;
    height: 38px;
    border-radius: 50%;
    border: none;
    background: #7c6af7;
    color: white;
    font-size: 16px;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
    transition: background 0.2s;

    &:hover { background: #5c4fcf; }
    &:disabled { background: #ccc; cursor: default; }
`;

// ─── Types ──────────────────────────────────────────────────────────────────────

interface ChatMessage {
    id: number;
    text: string;
    isUser: boolean;
}

const WELCOME: ChatMessage = {
    id: 0,
    text: '👋 Hi! I\'m your DataHub AI Assistant. Ask me anything about datasets, schemas, lineage, or privacy risk.',
    isUser: false,
};

// ─── Mock response (replace with real API call when orchestrator is ready) ──────

const MOCK_RESPONSES: string[] = [
    'The `user_events` table has 6 columns: `user_id`, `event_type`, `timestamp`, `session_id`, `raw_payload`, and `ip_address`. Note that `user_id` and `ip_address` are tagged as PII.',
    'Based on lineage analysis, this dataset has 12 upstream dependencies and feeds into 47 downstream pipelines. Changes here could have broad impact.',
    'Privacy Risk Score for `user_events`: 🔴 8/10 (High). Reasons: 2 PII columns, 47 downstream consumers, no data retention policy set.',
    'The owner of `user_events` is the **data-platform-team**. You can contact them via #data-platform in Slack.',
    'This table was last updated 3 hours ago. The most recent schema change was adding the `session_id` column on 2026-06-15.',
];

let mockIdx = 0;
const getMockResponse = () => {
    const response = MOCK_RESPONSES[mockIdx % MOCK_RESPONSES.length];
    mockIdx += 1;
    return response;
};

// ─── Component ──────────────────────────────────────────────────────────────────

export const AIChatButton: React.FC = () => {
    const [isOpen, setIsOpen] = useState(false);
    const [messages, setMessages] = useState<ChatMessage[]>([WELCOME]);
    const [inputText, setInputText] = useState('');
    const [isTyping, setIsTyping] = useState(false);
    const messagesEndRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    }, [messages, isTyping]);

    const sendMessage = async () => {
        const text = inputText.trim();
        if (!text || isTyping) return;

        const userMsg: ChatMessage = { id: Date.now(), text, isUser: true };
        setMessages((prev) => [...prev, userMsg]);
        setInputText('');
        setIsTyping(true);

        // TODO: Replace with real API call to orchestrator
        // const response = await fetch('/api/chat', {
        //     method: 'POST',
        //     headers: { 'Content-Type': 'application/json' },
        //     body: JSON.stringify({ message: text }),
        // });
        // const data = await response.json();

        // Simulated delay for demo
        setTimeout(() => {
            const aiMsg: ChatMessage = {
                id: Date.now() + 1,
                text: getMockResponse(),
                isUser: false,
            };
            setMessages((prev) => [...prev, aiMsg]);
            setIsTyping(false);
        }, 1200);
    };

    const handleKeyDown = (e: React.KeyboardEvent) => {
        if (e.key === 'Enter') sendMessage();
    };

    return (
        <>
            {isOpen && (
                <ChatPanel>
                    <PanelHeader>
                        <div>
                            <HeaderTitle>🤖 DataHub AI Assistant</HeaderTitle>
                        </div>
                        <CloseBtn onClick={() => setIsOpen(false)}>✕</CloseBtn>
                    </PanelHeader>

                    <MessagesArea>
                        {messages.map((msg) => (
                            <Message key={msg.id} $isUser={msg.isUser}>
                                {msg.text}
                            </Message>
                        ))}
                        {isTyping && <TypingIndicator>···</TypingIndicator>}
                        <div ref={messagesEndRef} />
                    </MessagesArea>

                    <InputArea>
                        <Input
                            placeholder="Ask about datasets, schemas, PII..."
                            value={inputText}
                            onChange={(e) => setInputText(e.target.value)}
                            onKeyDown={handleKeyDown}
                            autoFocus
                        />
                        <SendBtn onClick={sendMessage} disabled={!inputText.trim() || isTyping}>
                            ➤
                        </SendBtn>
                    </InputArea>
                </ChatPanel>
            )}

            <FloatingButton
                $isOpen={isOpen}
                onClick={() => setIsOpen((prev) => !prev)}
                title="Open AI Assistant"
            >
                {isOpen ? '✕' : '🤖'}
            </FloatingButton>
        </>
    );
};
