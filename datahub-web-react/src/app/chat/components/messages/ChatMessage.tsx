import { colors } from '@components';
import '@uiw/react-markdown-preview/dist/markdown.css';
import MDEditor from '@uiw/react-md-editor';
import React, { useState } from 'react';
import styled from 'styled-components';

import { CodeBlock } from '@app/chat/components/messages/CodeBlock';
import { MessageReferences } from '@app/chat/components/references/MessageReferences';
import { parseMessageContent } from '@app/chat/utils/parseMessageContent';

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

const MarkdownContent = styled.div<{ isUser: boolean }>`
    font-size: 15px;
    line-height: 1.7;
    /* Base color for all text - inherited by all child elements */
    color: ${colors.gray[600]};
    overflow-wrap: anywhere; /* Break words anywhere if needed */
    word-break: break-word; /* Break long words */

    /* Style markdown output - apply to both direct children and MDEditor rendered content */
    & p,
    & .wmde-markdown p {
        margin: 0 0 8px 0;
        overflow-wrap: anywhere; /* Break words anywhere if needed */
        word-break: break-word; /* Break long words if necessary */
        font-weight: 400; /* Regular weight */
        font-size: 15px; /* Match base font size */
    }

    & p:last-child,
    & .wmde-markdown p:last-child {
        margin-bottom: 0;
    }

    /* Paragraphs containing strong tags (headers) should have no bottom margin.
       The AI wraps header text (e.g., "**Header Text**") in <p><strong> tags,
       so we target the parent <p> to remove its default bottom margin and maintain
       consistent spacing between headers and content. */
    & p:has(strong),
    & .wmde-markdown p:has(strong) {
        margin-bottom: 0;
    }

    & strong,
    & .wmde-markdown strong {
        font-weight: 600;
        font-size: inherit; /* Use same size as parent text for inline bold */
        margin-bottom: 0px;
        /* Color inherited from parent */
    }

    & code,
    & .wmde-markdown code:not([class*='language-']) {
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

    /* Override MDEditor's link colors */
    & a,
    & .wmde-markdown a {
        color: ${colors.violet[600]} !important; /* Links use violet */
        text-decoration: underline;
        &:hover {
            text-decoration: underline;
        }
    }

    /* Ensure links inside headings are also violet */
    & h1 a,
    & h2 a,
    & h3 a,
    & h4 a,
    & h5 a,
    & h6 a,
    & .wmde-markdown h1 a,
    & .wmde-markdown h2 a,
    & .wmde-markdown h3 a,
    & .wmde-markdown h4 a,
    & .wmde-markdown h5 a,
    & .wmde-markdown h6 a {
        color: ${colors.violet[600]} !important;
        text-decoration: underline;
        &:hover {
            text-decoration: underline;
        }
    }

    & ul,
    & ol,
    & .wmde-markdown ul,
    & .wmde-markdown ol {
        margin: 0 0 8px 0;
        padding-left: 24px;
    }

    & li,
    & .wmde-markdown li {
        margin: 4px 0;
        font-weight: 400; /* Regular weight */
        font-size: 15px; /* Match base font size */
        color: ${colors.gray[600]}; /* Ensure list items are gray */
        overflow-wrap: anywhere;
        word-break: break-word;
        hyphens: auto;
    }

    & blockquote,
    & .wmde-markdown blockquote {
        border-left: 4px solid ${colors.gray[200]};
        padding-left: 16px;
        margin: 8px 0;
        color: ${colors.gray[600]};
        overflow-wrap: anywhere;
        word-break: break-word;
        hyphens: auto;
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
        overflow-wrap: anywhere;
        word-break: break-word;
        hyphens: auto;
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
        } catch {
            // Silently fail - clipboard API may not be available in some contexts
            // User will not see the "Copied" feedback if copy fails
        }
    };

    const renderContent = () => {
        const parts = parseMessageContent(message.content.text || '');

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
                            <CodeBlock
                                key={key}
                                language={part.language || 'code'}
                                content={part.content}
                                isTruncated={isTruncated}
                                isCopied={isCopied}
                                onCopy={() => handleCopyCode(part.content || '', index)}
                            />
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
                    <MessageReferences
                        messageText={message.content.text}
                        selectedEntityUrn={selectedEntityUrn}
                        onEntitySelect={onEntitySelect}
                    />
                )}
            </MessageContent>
        </MessageContainer>
    );
};
