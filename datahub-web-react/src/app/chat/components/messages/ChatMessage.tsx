import { colors } from '@components';
import { Check, Copy, Warning } from '@phosphor-icons/react';
import '@uiw/react-markdown-preview/dist/markdown.css';
import MDEditor from '@uiw/react-md-editor';
import React, { useState } from 'react';
import styled from 'styled-components';

import { MessageReferences } from '@app/chat/components/references/MessageReferences';

import {
    DataHubAiConversationActorType,
    DataHubAiConversationMessage,
    DataHubAiConversationMessageType,
    Entity,
} from '@types';

const MessageContainer = styled.div<{ isUser: boolean }>`
    display: flex;
    padding: 4px 24px 0px 24px;
    justify-content: ${(props) => (props.isUser ? 'flex-end' : 'flex-start')};
    margin: 0 0 32px 0;
`;

const MessageContent = styled.div<{ isUser: boolean }>`
    max-width: ${(props) => (props.isUser ? '70%' : '100%')};
    display: flex;
    flex-direction: column;
    align-items: ${(props) => (props.isUser ? 'flex-end' : 'flex-start')};
    width: ${(props) => (props.isUser ? 'auto' : '100%')};
    min-width: 0; /* Allow flex item to shrink below content size */
`;

const MessageBubble = styled.div<{ isUser: boolean }>`
    background-color: ${(props) => (props.isUser ? colors.gray[1600] : 'transparent')};
    color: ${(props) => (props.isUser ? colors.gray[600] : '#2d333a')};
    padding: ${(props) => (props.isUser ? '8px 8px' : '0')};
    border-radius: ${(props) => (props.isUser ? '12px' : '0')};
    word-wrap: break-word;
    overflow-wrap: break-word;
    width: 100%;
    max-width: 100%;
    min-width: 0; /* Allow content to shrink */
    overflow-x: auto; /* Allow horizontal scrolling for wide content */
    overflow-y: visible; /* Allow content to expand vertically */
`;

/* Code Block Container - matches Figma design */
const CodeBlockContainer = styled.div`
    display: flex;
    flex-direction: column;
    background: ${colors.white};
    border: 1px solid ${colors.gray[100]};
    box-shadow: 0px 4px 8px rgba(33, 23, 95, 0.04);
    border-radius: 12px;
    margin: 8px 0 24px 0;
    overflow: hidden;
    width: 100%;
`;

const CodeBlockHeader = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    align-items: center;
    padding: 12px 16px;
    height: 40px;
    background: ${colors.gray[1500]};
    border-bottom: 1px solid ${colors.gray[100]};
`;

const CodeBlockLanguageLabel = styled.span`
    font-family: 'Mulish', sans-serif;
    font-weight: 700;
    font-size: 12px;
    line-height: 15px;
    color: ${colors.gray[600]};
`;

const CopyButton = styled.button`
    display: flex;
    align-items: center;
    gap: 4px;
    padding: 4px 8px;
    background: transparent;
    border: none;
    border-radius: 4px;
    cursor: pointer;
    color: ${colors.gray[1800]};
    font-family: 'Mulish', sans-serif;
    font-size: 12px;
    font-weight: 600;
    transition: all 0.2s;

    &:hover {
        background: ${colors.gray[100]};
        color: ${colors.gray[600]};
    }

    &:active {
        background: ${colors.gray[200]};
    }
`;

const CodeBlockContent = styled.div`
    overflow-x: auto;

    & pre {
        margin: 0 !important;
        padding: 0 !important;
        background: transparent !important;
        border: none !important;
        border-radius: 0 !important;
        overflow: visible !important;
    }

    & code {
        font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
        font-size: 14px;
        line-height: 18px;
    }
`;

const TruncatedBanner = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 8px 8px;
    margin: 12px 16px;
    background: ${colors.yellow[0]};
    border-radius: 6px;
    font-family: 'Mulish', sans-serif;
    font-size: 14px;
    line-height: 20px;
    color: ${colors.yellow[1000]};
`;

const MarkdownContent = styled.div<{ isUser: boolean }>`
    font-size: 15px;
    line-height: 1.7;
    /* Base color for all text - inherited by all child elements */
    color: ${colors.gray[600]};
    overflow-wrap: anywhere; /* Break words anywhere if needed */
    word-break: break-word; /* Break long words */

    /* Style markdown output */
    & p {
        margin: 0 0 8px 0;
        overflow-wrap: anywhere; /* Break words anywhere if needed */
        word-break: break-word; /* Break long words if necessary */
        font-weight: 400; /* Regular weight */
        font-size: 15px; /* Match base font size */
    }

    & p:last-child {
        margin-bottom: 0;
    }

    /* Paragraphs containing strong tags (headers) should have no bottom margin */
    & p:has(strong) {
        margin-bottom: 0;
    }

    & strong {
        font-weight: 600;
        font-size: inherit; /* Use same size as parent text for inline bold */
        margin-bottom: 0px;
        /* Color inherited from parent */
    }

    & code {
        background-color: ${colors.gray[1500]};
        padding: 2px 6px;
        border-radius: 4px;
        font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
        font-size: 13px;
        word-wrap: break-word;
        overflow-wrap: break-word;
    }

    /* Don't hide any pre elements - let MDEditor render what our regex doesn't catch */

    /* Override MDEditor's black code color to allow syntax highlighting */
    & .wmde-markdown code[class*='language-'],
    & .wmde-markdown pre[class*='language-'] {
        color: inherit !important; /* Allow syntax highlighter to set colors */
    }

    & .wmde-markdown code {
        color: inherit !important; /* Base code inherits gray[600] */
    }

    /* Override MDEditor's heading colors */
    & .wmde-markdown h1,
    & .wmde-markdown h2,
    & .wmde-markdown h3,
    & .wmde-markdown h4,
    & .wmde-markdown h5,
    & .wmde-markdown h6 {
        color: ${colors.gray[600]} !important;
    }

    & a {
        color: ${colors.violet[600]}; /* Links use violet */
        text-decoration: underline;
        &:hover {
            text-decoration: underline;
        }
    }

    & ul,
    & ol {
        margin: 0 0 8px 0;
        padding-left: 24px;
    }

    & li {
        margin: 4px 0;
        font-weight: 400; /* Regular weight */
        font-size: 15px; /* Match base font size */
        /* Color inherited from parent */
    }

    & blockquote {
        border-left: 4px solid ${colors.gray[200]};
        padding-left: 16px;
        margin: 8px 0;
        color: ${colors.gray[600]};
    }

    & h1,
    & h2,
    & h3,
    & h4,
    & h5,
    & h6 {
        margin: 32px 0 8px 0;
        font-weight: 600;
        color: ${colors.gray[600]} !important;
    }

    /* Remove top margin from first heading */
    & h1:first-child,
    & h2:first-child,
    & h3:first-child,
    & h4:first-child,
    & h5:first-child,
    & h6:first-child {
        margin-top: 0;
    }

    & h1 {
        font-size: 20px;
    }

    & h2 {
        font-size: 18px;
    }

    & h3 {
        font-size: 16px;
    }

    /* Handle wide content like tables */
    & table {
        width: 100%;
        max-width: 100%;
        border-collapse: collapse;
        overflow-x: auto;
        display: block;
        white-space: nowrap;
    }

    & table thead,
    & table tbody,
    & table tr {
        display: table;
        width: 100%;
        table-layout: fixed;
    }

    & table td,
    & table th {
        word-wrap: break-word;
        overflow-wrap: break-word;
        max-width: 0;
    }

    /* Handle other wide elements */
    & img {
        max-width: 100%;
        height: auto;
    }

    & blockquote {
        overflow-wrap: anywhere;
        word-break: break-word;
        hyphens: auto;
    }

    /* Apply word breaking to all text elements */
    & h1,
    & h2,
    & h3,
    & h4,
    & h5,
    & h6 {
        overflow-wrap: anywhere;
        word-break: break-word;
        hyphens: auto;
    }

    & li {
        overflow-wrap: anywhere;
        word-break: break-word;
        hyphens: auto;
    }
`;

interface MessageRendererProps {
    message: DataHubAiConversationMessage;
    selectedEntityUrn?: string;
    onEntitySelect?: (entity: Entity | null) => void;
}

export const ChatMessage: React.FC<MessageRendererProps> = ({ message, selectedEntityUrn, onEntitySelect }) => {
    const isUser = message.actor.type === DataHubAiConversationActorType.User;
    const isThinking = message.type === DataHubAiConversationMessageType.Thinking;
    const isToolCall = message.type === DataHubAiConversationMessageType.ToolCall;
    const isToolResult = message.type === DataHubAiConversationMessageType.ToolResult;

    // Hooks must be called before any early returns
    const [copiedIndex, setCopiedIndex] = useState<number | null>(null);

    // Don't render thinking/tool messages - they're handled by ThinkingGroup
    if (isThinking || isToolCall || isToolResult) {
        return null;
    }

    const handleCopyCode = async (code: string, index: number) => {
        try {
            await navigator.clipboard.writeText(code);
            setCopiedIndex(index);
            setTimeout(() => setCopiedIndex(null), 2000);
        } catch (err) {
            console.error('Failed to copy code:', err);
        }
    };

    // Parse markdown to extract code blocks and regular content
    const parseMessageContent = () => {
        const text = message.content.text || '';
        const parts: Array<{ type: 'markdown' | 'code'; content: string; language?: string }> = [];

        // Regex to match complete code blocks
        const codeBlockRegex = /```(\w+)?[\s\n]*([\s\S]*?)```/g;
        let lastIndex = 0;
        let match = codeBlockRegex.exec(text);

        while (match !== null) {
            // Add markdown content before code block
            if (match.index > lastIndex) {
                const markdownText = text.substring(lastIndex, match.index);
                if (markdownText.trim()) {
                    parts.push({ type: 'markdown', content: markdownText });
                }
            }

            // Add complete code block
            parts.push({
                type: 'code',
                language: match[1] || 'code',
                content: match[2].trim(),
            });

            lastIndex = match.index + match[0].length;
            match = codeBlockRegex.exec(text);
        }

        // Check for incomplete/truncated code block at the end (missing closing ```)
        const remainingText = text.substring(lastIndex);
        const incompleteCodeMatch = remainingText.match(/```(\w+)?[\s\n]*([\s\S]*?)$/);

        if (incompleteCodeMatch) {
            // Add markdown content before incomplete code block
            const beforeIncomplete = remainingText.substring(0, incompleteCodeMatch.index);
            if (beforeIncomplete.trim()) {
                parts.push({ type: 'markdown', content: beforeIncomplete });
            }

            // Add incomplete code block with (Truncated) indicator
            const codeContent = incompleteCodeMatch[2].trim();
            if (codeContent) {
                parts.push({
                    type: 'code',
                    language: incompleteCodeMatch[1] || 'code',
                    content: codeContent,
                });
            }
        } else if (remainingText.trim()) {
            // No incomplete code block, just regular markdown
            parts.push({ type: 'markdown', content: remainingText });
        }

        // If no parts found, return the whole text as markdown
        if (parts.length === 0) {
            parts.push({ type: 'markdown', content: text });
        }

        return parts;
    };

    const renderContent = () => {
        const parts = parseMessageContent();

        return (
            <MarkdownContent isUser={isUser}>
                {parts.map((part, index) => {
                    // Generate a more stable key using content hash
                    const contentPreview = part.content.substring(0, 50).replace(/\s/g, '');
                    const key = `${part.type}-${index}-${contentPreview}`;

                    if (part.type === 'code') {
                        const isCopied = copiedIndex === index;
                        // Check if code ends with ... (truncated)
                        const isTruncated = part.content?.endsWith('...');

                        return (
                            <CodeBlockContainer key={key}>
                                <CodeBlockHeader>
                                    <CodeBlockLanguageLabel>{part.language?.toUpperCase()}</CodeBlockLanguageLabel>
                                    <CopyButton
                                        onClick={() => handleCopyCode(part.content || '', index)}
                                        title={isCopied ? 'Copied!' : 'Copy code'}
                                    >
                                        {isCopied ? (
                                            <>
                                                <Check size={14} weight="bold" />
                                                Copied
                                            </>
                                        ) : (
                                            <>
                                                <Copy size={14} weight="regular" />
                                                Copy
                                            </>
                                        )}
                                    </CopyButton>
                                </CodeBlockHeader>
                                <CodeBlockContent>
                                    <MDEditor.Markdown
                                        source={`\`\`\`${part.language}\n${part.content}\n\`\`\``}
                                        style={{ backgroundColor: 'transparent', color: 'inherit' }}
                                    />
                                </CodeBlockContent>
                                {isTruncated && (
                                    <TruncatedBanner>
                                        <Warning size={16} weight="fill" color={colors.yellow[500]} />
                                        <span>
                                            The query exceeds the maximum length. Try generating something shorter.
                                        </span>
                                    </TruncatedBanner>
                                )}
                            </CodeBlockContainer>
                        );
                    }
                    return (
                        <MDEditor.Markdown
                            key={key}
                            source={part.content}
                            style={{ backgroundColor: 'transparent', color: 'inherit' }}
                        />
                    );
                })}
            </MarkdownContent>
        );
    };

    const shouldShowReferences = !isUser && message.type === DataHubAiConversationMessageType.Text && !!onEntitySelect;

    return (
        <MessageContainer isUser={isUser}>
            <MessageContent isUser={isUser}>
                <MessageBubble isUser={isUser}>{renderContent()}</MessageBubble>
                {shouldShowReferences && (
                    <>
                        <MessageReferences
                            messageText={message.content.text}
                            selectedEntityUrn={selectedEntityUrn}
                            onEntitySelect={onEntitySelect}
                        />
                    </>
                )}
            </MessageContent>
        </MessageContainer>
    );
};
