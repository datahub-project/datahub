import { colors } from '@components';
import MDEditor from '@uiw/react-md-editor';
import React from 'react';
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
    padding: 4px 0px 0px 0px;
    justify-content: ${(props) => (props.isUser ? 'flex-end' : 'flex-start')};
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
    background-color: ${(props) => (props.isUser ? colors.primary[500] : 'transparent')};
    color: ${(props) => (props.isUser ? 'white' : '#2d333a')};
    padding: ${(props) => (props.isUser ? '12px 16px' : '0')};
    border-radius: ${(props) => (props.isUser ? '8px' : '0')};
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
    color: ${(props) => (props.isUser ? 'white' : '#2d333a')};
    overflow-wrap: anywhere; /* Break words anywhere if needed */
    word-break: break-word; /* Break long words */

    /* Style markdown output */
    & p {
        margin: 0 0 8px 0;
        overflow-wrap: anywhere; /* Break words anywhere if needed */
        word-break: break-word; /* Break long words if necessary */
    }

    & p:last-child {
        margin-bottom: 0;
    }

    & code {
        background-color: ${(props) => (props.isUser ? 'rgba(255, 255, 255, 0.2)' : 'rgba(0, 0, 0, 0.06)')};
        padding: 2px 6px;
        border-radius: 4px;
        font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
        font-size: 13px;
        word-wrap: break-word;
        overflow-wrap: break-word;
    }

    & pre {
        background-color: ${(props) => (props.isUser ? 'rgba(255, 255, 255, 0.1)' : '#f6f8fa')};
        padding: 12px;
        border-radius: 6px;
        overflow-x: auto;
        margin: 8px 0;
        max-width: 100%;
        width: 100%;
    }

    & pre code {
        background-color: transparent;
        padding: 0;
        white-space: pre;
        word-wrap: normal;
        overflow-wrap: normal;
    }

    & a {
        color: ${(props) => (props.isUser ? 'white' : colors.primary[500])};
        text-decoration: underline;
        &:hover {
            text-decoration: underline;
        }
    }

    & ul,
    & ol {
        margin: 8px 0;
        padding-left: 24px;
    }

    & li {
        margin: 4px 0;
    }

    & blockquote {
        border-left: 4px solid ${(props) => (props.isUser ? 'rgba(255, 255, 255, 0.5)' : colors.gray[100])};
        padding-left: 16px;
        margin: 8px 0;
        color: ${(props) => (props.isUser ? 'rgba(255, 255, 255, 0.9)' : '#595959')};
    }

    & h1,
    & h2,
    & h3,
    & h4,
    & h5,
    & h6 {
        margin: 16px 0 8px 0;
        font-weight: 600;
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

const Timestamp = styled.div<{ isUser: boolean }>`
    font-size: 11px;
    color: ${(props) => (props.isUser ? '#8c8c8c' : '#6e7781')};
    margin-top: 6px;
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

    // Don't render thinking/tool messages - they're handled by ThinkingGroup
    if (isThinking || isToolCall || isToolResult) {
        return null;
    }

    const formatTime = (timestamp: number) => {
        const date = new Date(timestamp);
        return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    };

    const renderContent = () => {
        return (
            <MarkdownContent isUser={isUser}>
                <MDEditor.Markdown
                    source={message.content.text}
                    style={{ backgroundColor: 'transparent', color: 'inherit' }}
                />
            </MarkdownContent>
        );
    };

    const shouldShowReferences = !isUser && message.type === DataHubAiConversationMessageType.Text && !!onEntitySelect;

    return (
        <MessageContainer isUser={isUser}>
            <MessageContent isUser={isUser}>
                <MessageBubble isUser={isUser}>{renderContent()}</MessageBubble>
                <Timestamp isUser={isUser}>{formatTime(message.time)}</Timestamp>
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
