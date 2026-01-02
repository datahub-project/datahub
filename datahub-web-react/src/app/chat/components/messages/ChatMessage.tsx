import { Button, Tooltip, colors } from '@components';
import MDEditor from '@uiw/react-md-editor';
import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';

import { MarkdownContent } from '@app/chat/components/messages/ChatMessage.styles';
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

const MessageContainer = styled.div<{ isUser: boolean }>`
    display: flex;
    padding: 0;
    justify-content: ${(props) => (props.isUser ? 'flex-end' : 'flex-start')};
    margin: 0;
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
    padding: ${(props) => (props.isUser ? '8px 8px' : '0')};
    border-radius: ${(props) => (props.isUser ? '12px' : '0')};
    word-wrap: break-word;
    overflow-wrap: break-word;
    width: 100%;
    max-width: 100%;
    min-width: 0; /* Allow content to shrink */
    overflow-y: visible; /* Allow content to expand vertically */
    box-sizing: border-box;
    ${(props) => props.$variant === ChatVariant.Compact && 'overflow-x: hidden;'}
`;

const ActionsContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    width: 100%;
    margin-top: 8px;
`;

// Compact mode: wrapper with hover-to-show action buttons
const CompactMessageWrapper = styled.div`
    width: 100%;

    &:hover .compact-action-buttons {
        opacity: 1;
        visibility: visible;
    }
`;

const CompactActionButtonsContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: flex-end;
    opacity: 0;
    visibility: hidden;
    transition:
        opacity 0.2s ease,
        visibility 0.2s ease;
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
        <MessageContainer ref={messageRef} isUser={isUser}>
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
