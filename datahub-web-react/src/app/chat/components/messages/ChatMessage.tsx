import { Button, Tooltip, colors } from '@components';
import MDEditor from '@uiw/react-md-editor';
import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';

import { CodeBlock } from '@app/chat/components/messages/CodeBlock';
import { MessageReferences } from '@app/chat/components/references/MessageReferences';
import { ChatMessageAction, ChatVariant } from '@app/chat/types';
import { parseMessageContent } from '@app/chat/utils/parseMessageContent';
import { extractTypeFromUrn } from '@app/entity/shared/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { PageRoutes } from '@conf/Global';

import {
    DataHubAiConversationActorType,
    DataHubAiConversationMessage,
    DataHubAiConversationMessageType,
    Entity,
} from '@types';

// Shared style object for MDEditor.Markdown (avoids recreating on every render)
const MARKDOWN_STYLE = { backgroundColor: 'transparent', color: 'inherit' };

const MessageContainer = styled.div<{ isUser: boolean; $variant?: ChatVariant }>`
    display: flex;
    padding: ${(props) => (props.$variant === ChatVariant.Compact ? '0' : '4px 0px 0px 0px')};
    justify-content: ${(props) => (props.isUser ? 'flex-end' : 'flex-start')};
    margin: ${(props) => (props.$variant === ChatVariant.Compact ? '0' : '0 0 32px 0')};
    width: 100%;
    max-width: 100%;
`;

const CopyButtonWrapper = styled.div`
    opacity: 0;
    visibility: hidden;
    transition:
        opacity 0.2s ease,
        visibility 0.2s ease;
    margin-left: auto;
    padding-top: 4px; /* Align with References header */
`;

const MessageContent = styled.div<{ isUser: boolean; $variant?: ChatVariant }>`
    max-width: ${(props) => {
        if (!props.isUser) return '100%';
        return props.$variant === ChatVariant.Compact ? '85%' : '70%';
    }};
    display: flex;
    flex-direction: column;
    align-items: ${(props) => (props.isUser ? 'flex-end' : 'flex-start')};
    width: ${(props) => (props.isUser ? 'auto' : '100%')};
    min-width: 0; /* Allow flex item to shrink below content size */
    box-sizing: border-box;

    &:hover ${CopyButtonWrapper} {
        opacity: 1;
        visibility: visible;
    }
`;

const MessageBubble = styled.div<{ isUser: boolean; $variant?: ChatVariant }>`
    background-color: ${(props) => (props.isUser ? colors.gray[1600] : 'transparent')};
    color: ${(props) => (props.isUser ? colors.gray[600] : '#2d333a')};
    padding: ${(props) => {
        if (!props.isUser) return '0';
        return props.$variant === ChatVariant.Compact ? '8px 12px' : '8px 8px';
    }};
    border-radius: ${(props) => (props.isUser ? '12px' : '0')};
    word-wrap: break-word;
    overflow-wrap: break-word;
    width: 100%;
    max-width: 100%;
    min-width: 0; /* Allow content to shrink */
    /* Compact mode clips overflow to prevent horizontal scroll in narrow sidebar */
    overflow-x: ${(props) => (props.$variant === ChatVariant.Compact ? 'hidden' : 'auto')};
    overflow-y: visible; /* Allow content to expand vertically */
    box-sizing: border-box;
`;

const ActionsContainer = styled.div`
    display: flex;
    align-items: flex-start;
    gap: 8px;
    width: 100%;
    margin-top: 8px;
`;

// Compact mode: wrapper with hover-to-show action buttons
const CompactMessageWrapper = styled.div`
    position: relative;
    width: 100%;

    &:hover .compact-action-buttons {
        opacity: 1;
        visibility: visible;
    }
`;

const CompactActionButtonsContainer = styled.div`
    position: absolute;
    top: 100%;
    right: 0;
    display: flex;
    align-items: center;
    gap: 0px;
    opacity: 0;
    visibility: hidden;
    transition:
        opacity 0.2s ease,
        visibility 0.2s ease;
    z-index: 1;
`;

const MarkdownContent = styled.div<{ isUser: boolean; $variant?: ChatVariant }>`
    font-size: 14px;
    line-height: 1.7;
    /* Base color for all text - inherited by all child elements */
    color: ${colors.gray[600]};
    overflow-wrap: anywhere; /* Break words anywhere if needed */
    word-break: break-word; /* Break long words */
    ${(props) =>
        props.$variant === ChatVariant.Compact &&
        `
        white-space: normal;
        width: 100%;
        max-width: 100%;
        min-width: 0;
        box-sizing: border-box;
    `}

    /* Override markdown renderer defaults in compact mode */
    ${(props) =>
        props.$variant === ChatVariant.Compact &&
        `
        & .wmde-markdown {
            white-space: normal !important;
            overflow-wrap: anywhere !important;
            word-break: break-word !important;
            max-width: 100% !important;
        }
    `}

    /* Style markdown output - apply to both direct children and MDEditor rendered content */
    & p,
    & .wmde-markdown p {
        margin: 0 0 8px 0;
        overflow-wrap: anywhere !important; /* Break words anywhere if needed */
        word-break: break-word !important; /* Break long words if necessary */
        ${(props) => props.$variant === ChatVariant.Compact && 'white-space: normal !important;'}
        font-weight: 400; /* Regular weight */
        font-size: 14px;
        max-width: 100%;
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
        font-size: 14px;
        color: ${colors.gray[600]}; /* Ensure list items are gray */
        overflow-wrap: anywhere;
        word-break: break-word;
        ${(props) => props.$variant === ChatVariant.Compact && 'white-space: normal !important;'}
        hyphens: auto;
    }

    & blockquote {
        border-left: 4px solid ${(props) => (props.isUser ? 'rgba(255, 255, 255, 0.5)' : colors.gray[100])};
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
    conversationUrn?: string;
    selectedEntityUrn?: string;
    onEntitySelect?: (entity: Entity | null) => void;
    variant?: ChatVariant; // 'full' shows MessageReferences, 'compact' for entity sidebar
    allowedActions?: ChatMessageAction[];
    showReferences?: boolean;
}

export const ChatMessage: React.FC<MessageRendererProps> = ({
    message,
    conversationUrn,
    selectedEntityUrn,
    onEntitySelect,
    variant = ChatVariant.Full,
    allowedActions,
    showReferences = true,
}) => {
    const isUser = message.actor.type === DataHubAiConversationActorType.User;
    const isThinking = message.type === DataHubAiConversationMessageType.Thinking;
    const isToolCall = message.type === DataHubAiConversationMessageType.ToolCall;
    const isToolResult = message.type === DataHubAiConversationMessageType.ToolResult;

    // Hooks must be called before any early returns
    const [copiedIndex, setCopiedIndex] = useState<number | null>(null);
    const [copied, setCopied] = useState(false);
    const entityRegistry = useEntityRegistry();
    const history = useHistory();
    const messageRef = useRef<HTMLDivElement>(null);

    // Memoize parsing to avoid re-parsing on every render (matches pattern used in MessageReferences)
    const parts = useMemo(() => {
        return parseMessageContent(message.content.text || '');
    }, [message.content.text]);

    // Intercept encoded URN links and route via entityRegistry so in-app navigation is correct
    // Format: /{entityType}/{urlEncodedUrn} (e.g., /dataset/urn%3Ali%3Adataset%3A...)
    useEffect(() => {
        const handleLinkClick = (e: MouseEvent) => {
            const target = e.target as HTMLElement;
            const link = target.closest('a');
            if (!link?.href) {
                return;
            }

            const href = link.getAttribute('href');
            const isUrnLink = href && (href.startsWith('urn%3Ali%3A') || href.startsWith('urn:li:'));
            if (!isUrnLink) {
                return;
            }

            try {
                const decodedUrn = decodeURIComponent(href!);
                const entityType = extractTypeFromUrn(decodedUrn);
                if (!entityType) {
                    return;
                }
                e.preventDefault();
                e.stopPropagation();
                const entityUrl = entityRegistry.getEntityUrl(entityType, decodedUrn);
                history.push(entityUrl);
            } catch {
                // Let browser handle default navigation if parsing fails
            }
        };

        const container = messageRef.current;
        if (!container) {
            return undefined;
        }
        container.addEventListener('click', handleLinkClick);
        return () => container.removeEventListener('click', handleLinkClick);
    }, [entityRegistry, history]);

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

    const content = useMemo(() => {
        return (
            <MarkdownContent isUser={isUser} $variant={variant}>
                {parts.map((part, index) => {
                    const contentPreview = part.content.substring(0, 50).replace(/\s/g, '');
                    const key = `${part.type}-${index}-${contentPreview}`;

                    if (part.type === 'code') {
                        const isCopied = copiedIndex === index;
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
                    return <MDEditor.Markdown key={key} source={part.content} style={MARKDOWN_STYLE} />;
                })}
            </MarkdownContent>
        );
    }, [parts, isUser, copiedIndex, variant]);

    const allowedActionsSet = useMemo(
        () => new Set<ChatMessageAction>(allowedActions || [ChatMessageAction.Copy, ChatMessageAction.OpenInChat]),
        [allowedActions],
    );
    const isAiTextMessage = !isUser && message.type === DataHubAiConversationMessageType.Text;
    const canShowReferences = showReferences && !!onEntitySelect && isAiTextMessage;

    const messageText = message.content?.text || '';

    const handleCopy = async () => {
        if (!messageText || !navigator.clipboard?.writeText) {
            return;
        }
        try {
            await navigator.clipboard.writeText(messageText);
            setCopied(true);
            setTimeout(() => setCopied(false), 2000);
        } catch {
            // Swallow errors to avoid breaking UX; matches handleCopyCode semantics
        }
    };

    const canShowCopy = allowedActionsSet.has(ChatMessageAction.Copy) && isAiTextMessage;
    const canShowOpenInChat =
        allowedActionsSet.has(ChatMessageAction.OpenInChat) && isAiTextMessage && !!conversationUrn;

    const handleOpenInChat = () => {
        if (conversationUrn) {
            history.push(`${PageRoutes.AI_CHAT}?conversation=${conversationUrn}`);
        }
    };

    const shouldShowFullActions = variant === ChatVariant.Full && (canShowReferences || canShowCopy);
    const shouldShowCompactActions = variant === ChatVariant.Compact && (canShowCopy || canShowOpenInChat);

    const messageElement = (
        <MessageContainer ref={messageRef} isUser={isUser} $variant={variant}>
            <MessageContent isUser={isUser} $variant={variant}>
                <MessageBubble isUser={isUser} $variant={variant}>
                    {content}
                </MessageBubble>
                {shouldShowFullActions && (
                    <ActionsContainer>
                        {canShowReferences && (
                            <MessageReferences
                                messageText={message.content.text}
                                selectedEntityUrn={selectedEntityUrn}
                                onEntitySelect={onEntitySelect}
                            />
                        )}
                        {canShowCopy && (
                            <CopyButtonWrapper>
                                <Tooltip title={copied ? 'Copied!' : 'Copy'} placement="top">
                                    <Button
                                        variant="text"
                                        size="md"
                                        color="gray"
                                        onClick={handleCopy}
                                        icon={{
                                            icon: copied ? 'Check' : 'Copy',
                                            source: 'phosphor',
                                            size: 'md',
                                        }}
                                        style={{ padding: '4px 8px', minWidth: 'auto' }}
                                    />
                                </Tooltip>
                            </CopyButtonWrapper>
                        )}
                    </ActionsContainer>
                )}
            </MessageContent>
        </MessageContainer>
    );

    if (isThinking || isToolCall || isToolResult) {
        return null;
    }

    // Compact mode with action buttons: wrap in hover container
    if (shouldShowCompactActions) {
        return (
            <CompactMessageWrapper>
                {messageElement}
                <CompactActionButtonsContainer className="compact-action-buttons">
                    {canShowCopy && (
                        <Tooltip title={copied ? 'Copied!' : 'Copy'} placement="top">
                            <Button
                                variant="text"
                                size="md"
                                color="gray"
                                onClick={handleCopy}
                                icon={{
                                    icon: copied ? 'Check' : 'Copy',
                                    source: 'phosphor',
                                    size: 'md',
                                }}
                                style={{ padding: '4px 8px', minWidth: 'auto' }}
                            />
                        </Tooltip>
                    )}
                    {canShowOpenInChat && (
                        <Tooltip title="Open in Chat" placement="top">
                            <Button
                                variant="text"
                                size="md"
                                color="gray"
                                onClick={handleOpenInChat}
                                icon={{ icon: 'ArrowUpRight', source: 'phosphor', size: 'md' }}
                                style={{ padding: '4px 8px', minWidth: 'auto' }}
                            />
                        </Tooltip>
                    )}
                </CompactActionButtonsContainer>
            </CompactMessageWrapper>
        );
    }

    return messageElement;
};
