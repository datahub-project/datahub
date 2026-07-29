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

// Model picker lives in the chat panel (not settings) so it can be switched per-conversation.
const ModelSelect = styled.select`
    background: rgba(255, 255, 255, 0.15);
    color: white;
    border: 1px solid rgba(255, 255, 255, 0.3);
    border-radius: 6px;
    font-size: 11px;
    padding: 3px 6px;
    margin-top: 4px;
    cursor: pointer;
    &:focus { outline: none; }
    option { color: #333; }
`;

type ChatModelOption = { value: string; label: string };

const MODEL_OPTIONS_BY_ENUM: Record<string, ChatModelOption> = {
    SONNET: { value: 'claude-sonnet-5', label: 'Claude Sonnet 5' },
    OPUS: { value: 'claude-opus-4-8', label: 'Claude Opus 4.8' },
    GPT_5_5: { value: 'gpt-5-5', label: 'GPT 5.5' },
};

const FALLBACK_CHAT_MODELS: ChatModelOption[] = [
    MODEL_OPTIONS_BY_ENUM.SONNET,
    MODEL_OPTIONS_BY_ENUM.OPUS,
];

type PreferredModelResponse = {
    model?: string | null;
};

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

interface PageContext {
    pageUrl: string;
    pageType: string;        // e.g. "dataset", "dashboard", "domain", "policy", "home"
    entityUrn?: string;      // e.g. "urn:li:dataset:(urn:li:dataPlatform:hive,users,PROD)"
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

const AI_CHAT_ENDPOINT =
    (import.meta as any)?.env?.VITE_AI_CHAT_ENDPOINT || 'http://localhost:8000/api/ai/chat';

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

export const AIChatButton: React.FC = () => {
    const [isOpen, setIsOpen] = useState(false);
    const [messages, setMessages] = useState<ChatMessage[]>([WELCOME]);
    const [inputText, setInputText] = useState('');
    const [isTyping, setIsTyping] = useState(false);
    const [sessionId] = useState<string>(() => crypto.randomUUID());
    const [availableModels, setAvailableModels] = useState<ChatModelOption[]>(FALLBACK_CHAT_MODELS);
    const [model, setModel] = useState(FALLBACK_CHAT_MODELS[0].value);
    const messagesEndRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    }, [messages, isTyping]);

    useEffect(() => {
        let isMounted = true;

        const loadModels = async () => {
            try {
                const [modelsResponse, preferredModelResponse] = await Promise.all([
                    fetch(resolveRuntimePath('/api/ai-config/models')),
                    fetch(resolveRuntimePath('/api/ai-config/preferred-model')),
                ]);
                if (!modelsResponse.ok) return;

                const data = (await modelsResponse.json()) as { models?: string[] };
                const preferredModelData = preferredModelResponse.ok
                    ? ((await preferredModelResponse.json()) as PreferredModelResponse)
                    : null;
                const nextModels = data.models
                    ?.map((modelName) => MODEL_OPTIONS_BY_ENUM[modelName])
                    .filter((option): option is ChatModelOption => Boolean(option)) || [];

                if (!isMounted || nextModels.length === 0) return;

                const preferredModel = preferredModelData?.model || null;
                setAvailableModels(nextModels);
                setModel((currentModel) =>
                    preferredModel && nextModels.some((option) => option.value === preferredModel)
                        ? preferredModel
                        : nextModels.some((option) => option.value === currentModel)
                        ? currentModel
                        : nextModels[0].value,
                );
            } catch {
                // keep fallback chat models if backend request fails
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
                    model,                        // model chosen in the chat header
                    context: getPageContext(),   // current page URL + entity type
                    session_id: sessionId,        // persistent session for conversation memory
                }),
            });

            if (!response.ok || !response.body) {
                throw new Error(`Backend returned ${response.status}`);
            }

            const reader = response.body.getReader();
            const decoder = new TextDecoder();
            const aiMsgId = Date.now() + 1;
            let accumulated = '';

            setMessages((prev) => [...prev, { id: aiMsgId, text: '', isUser: false }]);
            setIsTyping(false);

            // Stream response token-by-token (SSE JSON events: data: {"token": "..."})
            while (true) {
                const { done, value } = await reader.read();
                if (done) break;

                const chunk = decoder.decode(value, { stream: true });
                // Parse SSE lines: "data: {"token": "hello"}\n"
                for (const line of chunk.split('\n')) {
                    if (!line.startsWith('data: ')) continue;
                    const payload = line.slice(6).trim();
                    if (payload === '[DONE]') break;
                    try {
                        const { token } = JSON.parse(payload) as { token: string };
                        accumulated += token;
                        // Update the message bubble live as each token arrives
                        setMessages((prev) =>
                            prev.map((m) => (m.id === aiMsgId ? { ...m, text: accumulated } : m)),
                        );
                    } catch {
                        // skip malformed lines
                    }
                }
            }
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

    const handleModelChange = async (nextModel: string) => {
        setModel(nextModel);

        try {
            await fetch(resolveRuntimePath('/api/ai-config/preferred-model'), {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ model: nextModel }),
            });
        } catch {
            // keep the local selection even if persistence fails
        }
    };

    return (
        <>
            {isOpen && (
                <ChatPanel>
                    <PanelHeader>
                        <div>
                            <HeaderTitle>🤖 DataHub AI Assistant</HeaderTitle>
                            <ModelSelect
                                value={model}
                                onChange={(e) => void handleModelChange(e.target.value)}
                                title="Choose the model for this conversation"
                            >
                                {availableModels.map((m) => (
                                    <option key={m.value} value={m.value}>
                                        {m.label}
                                    </option>
                                ))}
                            </ModelSelect>
                        </div>
                        <CloseBtn onClick={() => setIsOpen(false)}>✕</CloseBtn>
                    </PanelHeader>

                    <MessagesArea>
                        {messages.map((msg) => (
                            <Message key={msg.id} $isUser={msg.isUser}>
                                {msg.isUser ? (
                                    msg.text
                                ) : (
                                    <MarkdownContent>
                                        <ReactMarkdown remarkPlugins={[remarkGfm]}>{msg.text}</ReactMarkdown>
                                    </MarkdownContent>
                                )}
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
