import React, { useState, useRef, useEffect } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import styled, { keyframes } from 'styled-components';

import { resolveRuntimePath } from '@utils/runtimeBasePath';

// ─── Animations ────────────────────────────────────────────────────────────────

const slideIn = keyframes`
    from { opacity: 0; transform: translateY(20px) scale(0.95); }
    to   { opacity: 1; transform: translateY(0) scale(1); }
`;

const HEADER_TITLE = 'DataHub AI Assistant';
const MODEL_SELECT_TITLE = 'Choose the model for this conversation';
const CLOSE_BUTTON_LABEL = 'Close AI Assistant';
const OPEN_BUTTON_LABEL = 'Open AI Assistant';
const INPUT_PLACEHOLDER = 'Ask about datasets, schemas, PII...';
const SEND_BUTTON_LABEL = 'Send message';
const TYPING_INDICATOR = '...';
const CHAT_OPEN_ICON = '🤖';
const CHAT_CLOSE_ICON = '✕';
const SEND_ICON = '➤';

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
    background: ${({ $isOpen, theme }) =>
        $isOpen ? theme.colors.buttonSurfaceBrandHover : theme.colors.buttonFillBrand};
    color: ${(props) => props.theme.colors.textOnFillBrand};
    font-size: 22px;
    cursor: pointer;
    box-shadow: ${(props) => props.theme.colors.shadowLg};
    display: flex;
    align-items: center;
    justify-content: center;
    transition:
        background 0.2s,
        transform 0.2s,
        box-shadow 0.2s;

    &:hover {
        background: ${(props) => props.theme.colors.buttonSurfaceBrandHover};
        transform: scale(1.08);
        box-shadow: ${(props) => props.theme.colors.shadowXl};
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
    background: ${(props) => props.theme.colors.bg};
    border-radius: 16px;
    box-shadow: ${(props) => props.theme.colors.shadowXl};
    display: flex;
    flex-direction: column;
    overflow: hidden;
    animation: ${slideIn} 0.22s ease;
`;

const PanelHeader = styled.div`
    background: ${(props) => props.theme.colors.brandGradient};
    padding: 16px 18px;
    color: ${(props) => props.theme.colors.textOnFillBrand};
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

const CloseBtn = styled.button`
    background: none;
    border: none;
    color: ${(props) => props.theme.colors.textOnFillBrand};
    font-size: 18px;
    cursor: pointer;
    padding: 2px 6px;
    border-radius: 4px;
    opacity: 0.8;
    &:hover {
        opacity: 1;
        background: ${(props) => props.theme.colors.overlayOnBrand};
    }
`;

// Model picker lives in the chat panel (not settings) so it can be switched per-conversation.
const ModelSelect = styled.select`
    background: ${(props) => props.theme.colors.overlayOnBrand};
    color: ${(props) => props.theme.colors.textOnFillBrand};
    border: 1px solid ${(props) => props.theme.colors.borderBrandInverse};
    border-radius: 6px;
    font-size: 11px;
    padding: 3px 6px;
    margin-top: 4px;
    cursor: pointer;
    &:focus {
        outline: none;
    }
    option {
        color: ${(props) => props.theme.colors.text};
        background: ${(props) => props.theme.colors.bg};
    }
`;

type ChatModelOption = {
    value: string;
    label: string;
};

const MODEL_OPTIONS_BY_ENUM: Record<string, ChatModelOption> = {
    SONNET: { value: 'claude-sonnet-5', label: 'Claude Sonnet 5' },
    OPUS: { value: 'claude-opus-4-8', label: 'Claude Opus 4.8' },
    GPT_5_5: { value: 'gpt-5-5', label: 'GPT 5.5' },
};

const FALLBACK_CHAT_MODELS: ChatModelOption[] = [
    MODEL_OPTIONS_BY_ENUM.SONNET,
    MODEL_OPTIONS_BY_ENUM.OPUS,
];

const MessagesArea = styled.div`
    flex: 1;
    overflow-y: auto;
    padding: 16px;
    display: flex;
    flex-direction: column;
    gap: 12px;
    background: ${(props) => props.theme.colors.bgSurface};
`;

const Message = styled.div<{ $isUser: boolean }>`
    max-width: 85%;
    padding: 10px 14px;
    border-radius: ${({ $isUser }) => ($isUser ? '16px 16px 4px 16px' : '16px 16px 16px 4px')};
    background: ${({ $isUser, theme }) =>
        $isUser ? theme.colors.buttonFillBrand : theme.colors.bg};
    color: ${({ $isUser, theme }) => ($isUser ? theme.colors.textOnFillBrand : theme.colors.text)};
    font-size: 13.5px;
    line-height: 1.5;
    align-self: ${({ $isUser }) => ($isUser ? 'flex-end' : 'flex-start')};
    box-shadow: ${(props) => props.theme.colors.shadowXs};
`;

// Renders markdown from the assistant (bold, lists, tables, code, links) inside the chat bubble.
const MarkdownContent = styled.div`
    /* Tighten spacing so markdown fits the narrow chat panel */
    & > *:first-child { margin-top: 0; }
    & > *:last-child { margin-bottom: 0; }
    p { margin: 0 0 8px; }
    ul, ol { margin: 0 0 8px; padding-left: 20px; }
    li { margin: 2px 0; }
    h1, h2, h3, h4 { margin: 8px 0 4px; font-size: 14px; font-weight: 600; }
    a { color: #7c6af7; text-decoration: underline; }
    code {
        background: #f0f0f5;
        padding: 1px 5px;
        border-radius: 4px;
        font-size: 12px;
        font-family: 'SFMono-Regular', Consolas, monospace;
    }
    pre {
        background: #f0f0f5;
        padding: 10px;
        border-radius: 6px;
        overflow-x: auto;
        margin: 0 0 8px;
    }
    pre code { background: none; padding: 0; }
    table {
        border-collapse: collapse;
        width: 100%;
        font-size: 12px;
        margin: 0 0 8px;
    }
    th, td { border: 1px solid #e0e0e8; padding: 4px 8px; text-align: left; }
    th { background: #f7f7fb; font-weight: 600; }
    blockquote {
        border-left: 3px solid #d0d0dc;
        margin: 0 0 8px;
        padding-left: 10px;
        color: #555;
    }
`;

const TypingIndicator = styled.div`
    align-self: flex-start;
    background: ${(props) => props.theme.colors.bg};
    border-radius: 16px 16px 16px 4px;
    padding: 10px 16px;
    font-size: 20px;
    letter-spacing: 2px;
    color: ${(props) => props.theme.colors.text};
    box-shadow: ${(props) => props.theme.colors.shadowXs};
`;

const InputArea = styled.div`
    padding: 12px 14px;
    border-top: 1px solid ${(props) => props.theme.colors.border};
    display: flex;
    gap: 8px;
    background: ${(props) => props.theme.colors.bg};
    flex-shrink: 0;
`;

const Input = styled.input`
    flex: 1;
    border: 1.5px solid ${(props) => props.theme.colors.borderInput};
    border-radius: 22px;
    padding: 9px 16px;
    font-size: 13.5px;
    outline: none;
    background: ${(props) => props.theme.colors.bgInput};
    color: ${(props) => props.theme.colors.text};

    &:focus {
        border-color: ${(props) => props.theme.colors.borderInputFocus};
        background: ${(props) => props.theme.colors.bg};
    }

    &::placeholder {
        color: ${(props) => props.theme.colors.textPlaceholder};
    }
`;

const SendBtn = styled.button`
    width: 38px;
    height: 38px;
    border-radius: 50%;
    border: none;
    background: ${(props) => props.theme.colors.buttonFillBrand};
    color: ${(props) => props.theme.colors.textOnFillBrand};
    font-size: 16px;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
    transition: background 0.2s;

    &:hover {
        background: ${(props) => props.theme.colors.buttonSurfaceBrandHover};
    }
    &:disabled {
        background: ${(props) => props.theme.colors.bgDisabled};
        cursor: default;
    }
`;

// ─── Types ──────────────────────────────────────────────────────────────────────

interface ChatMessage {
    id: number;
    text: string;
    isUser: boolean;
}

const WELCOME: ChatMessage = {
    id: 0,
    text: "👋 Hi! I'm your DataHub AI Assistant. Ask me anything about datasets, schemas, lineage, or privacy risk.",
    isUser: false,
};

// ─── Page context — automatically read from the current browser URL ─────────────

interface PageContext {
    pageUrl: string;
    pageType: string; // e.g. "dataset", "dashboard", "domain", "policy", "home"
    entityUrn?: string; // e.g. "urn:li:dataset:(urn:li:dataPlatform:hive,users,PROD)"
}

const getPageContext = (): PageContext => {
    const path = window.location.pathname;
    const parts = path.split('/').filter(Boolean);
    return {
        pageUrl: window.location.href,
        pageType: parts[0] || 'home',
        entityUrn: parts[1] ? decodeURIComponent(parts[1]) : undefined,
    };
};

// ─── AI endpoint — points at the local AI Orchestrator (see ai-orchestrator/) ───
// Override at build time with VITE_AI_CHAT_ENDPOINT if needed.
const AI_CHAT_ENDPOINT =
    (import.meta as any)?.env?.VITE_AI_CHAT_ENDPOINT || 'http://localhost:8000/api/ai/chat';

// ─── Mock fallback (used when backend is unavailable) ───────────────────────────

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

const applyTokenToMessage = (
    aiMsgId: number,
    tokenText: string,
    setMessages: React.Dispatch<React.SetStateAction<ChatMessage[]>>,
) => {
    setMessages((prev) =>
        prev.map((message) => (message.id === aiMsgId ? { ...message, text: tokenText } : message)),
    );
};

const processSseChunk = (chunk: string, onToken: (token: string) => void) => {
    let receivedDone = false;

    chunk.split('\n').forEach((line) => {
        if (receivedDone || !line.startsWith('data: ')) {
            return;
        }

        const payload = line.slice(6).trim();
        if (payload === '[DONE]') {
            receivedDone = true;
            return;
        }

        try {
            const { token } = JSON.parse(payload) as { token: string };
            onToken(token);
        } catch {
            // Skip malformed SSE lines.
        }
    });

    return receivedDone;
};

// ─── Component ──────────────────────────────────────────────────────────────────

export const AIChatButton: React.FC = () => {
    const [isOpen, setIsOpen] = useState(false);
    const [messages, setMessages] = useState<ChatMessage[]>([WELCOME]);
    const [inputText, setInputText] = useState('');
    const [isTyping, setIsTyping] = useState(false);
    // One UUID per browser tab — gives Claude memory within a session; resets on tab close
    const [sessionId] = useState<string>(() => crypto.randomUUID());
    const [availableModels, setAvailableModels] =
        useState<ChatModelOption[]>(FALLBACK_CHAT_MODELS);
    const [model, setModel] = useState(FALLBACK_CHAT_MODELS[0].value);
    const messagesEndRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    }, [messages, isTyping]);

    useEffect(() => {
        let isMounted = true;

        const loadModels = async () => {
            try {
                const response = await fetch(resolveRuntimePath('/api/ai-config/models'));
                if (!response.ok) {
                    return;
                }

                const data = (await response.json()) as { models?: string[] };
                const nextModels =
                    data.models
                        ?.map((modelName) => MODEL_OPTIONS_BY_ENUM[modelName])
                        .filter((option): option is ChatModelOption => Boolean(option)) || [];

                if (!isMounted || nextModels.length === 0) {
                    return;
                }

                setAvailableModels(nextModels);
                setModel((currentModel) =>
                    nextModels.some((option) => option.value === currentModel) ? currentModel : nextModels[0].value,
                );
            } catch {
                // Keep fallback chat models if the backend request fails.
            }
        };

        loadModels();

        return () => {
            isMounted = false;
        };
    }, []);

    const sendMessage = async () => {
        const text = inputText.trim();
        if (!text || isTyping) return;

        const userMsg: ChatMessage = { id: Date.now(), text, isUser: true };
            setMessages((prev) => [...prev, userMsg]);
        setInputText('');
        setIsTyping(true);

        try {
            // ── Real SSE streaming call to backend ───────────────────────────
            const response = await fetch(AI_CHAT_ENDPOINT, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    message: text,
                    model, // model chosen in the chat header
                    context: getPageContext(), // current page URL + entity type
                    session_id: sessionId, // persistent session for conversation memory
                }),
            });

            if (!response.ok || !response.body) {
                throw new Error(`Backend returned ${response.status}`);
            }

            // Stream response token-by-token (SSE JSON events: data: {"token": "..."})
            const reader = response.body.getReader();
            const decoder = new TextDecoder();
            const aiMsgId = Date.now() + 1;
            let accumulated = '';

            // Add empty AI message bubble — we'll fill it as tokens arrive
            setMessages((prev) => [...prev, { id: aiMsgId, text: '', isUser: false }]);
            setIsTyping(false);

            const readNextChunk = async (): Promise<void> => {
                const { done, value } = await reader.read();
                if (done) {
                    return;
                }

                const chunk = decoder.decode(value, { stream: true });
                const isDone = processSseChunk(chunk, (token) => {
                    accumulated += token;
                    applyTokenToMessage(aiMsgId, accumulated, setMessages);
                });

                if (!isDone) {
                    await readNextChunk();
                }
            };

            await readNextChunk();
        } catch {
            // ── Fallback to mock if backend is unavailable ───────────────────
            setTimeout(() => {
                setMessages((prev) => [
                    ...prev,
                    { id: Date.now() + 1, text: getMockResponse(), isUser: false },
                ]);
                setIsTyping(false);
            }, 1200);
        }
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
                            <HeaderTitle>
                                <span>{CHAT_OPEN_ICON}</span>
                                <span>{HEADER_TITLE}</span>
                            </HeaderTitle>
                            <ModelSelect
                                value={model}
                                onChange={(e) => setModel(e.target.value)}
                                title={MODEL_SELECT_TITLE}
                            >
                                {availableModels.map((m) => (
                                    <option key={m.value} value={m.value}>
                                        {m.label}
                                    </option>
                                ))}
                            </ModelSelect>
                        </div>
                        <CloseBtn aria-label={CLOSE_BUTTON_LABEL} onClick={() => setIsOpen(false)}>
                            {CHAT_CLOSE_ICON}
                        </CloseBtn>
                    </PanelHeader>

                    <MessagesArea>
                        {messages.map((msg) => (
                            <Message key={msg.id} $isUser={msg.isUser}>
                                {msg.isUser ? (
                                    msg.text
                                ) : (
                                    <MarkdownContent>
                                        <ReactMarkdown remarkPlugins={[remarkGfm]}>
                                            {msg.text}
                                        </ReactMarkdown>
                                    </MarkdownContent>
                                )}
                            </Message>
                        ))}
                        {isTyping && <TypingIndicator>{TYPING_INDICATOR}</TypingIndicator>}
                        <div ref={messagesEndRef} />
                    </MessagesArea>

                    <InputArea>
                        <Input
                            placeholder={INPUT_PLACEHOLDER}
                            value={inputText}
                            onChange={(e) => setInputText(e.target.value)}
                            onKeyDown={handleKeyDown}
                            autoFocus
                        />
                        <SendBtn
                            aria-label={SEND_BUTTON_LABEL}
                            onClick={sendMessage}
                            disabled={!inputText.trim() || isTyping}
                        >
                            {SEND_ICON}
                        </SendBtn>
                    </InputArea>
                </ChatPanel>
            )}

            <FloatingButton
                $isOpen={isOpen}
                onClick={() => setIsOpen((prev) => !prev)}
                title={OPEN_BUTTON_LABEL}
            >
                {isOpen ? CHAT_CLOSE_ICON : CHAT_OPEN_ICON}
            </FloatingButton>
        </>
    );
};
