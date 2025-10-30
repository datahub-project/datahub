import { Button, Loader, colors } from '@components';
import { Lightning } from '@phosphor-icons/react';
import MDEditor from '@uiw/react-md-editor';
import React, { useEffect, useRef, useState } from 'react';
import styled from 'styled-components';

import { DataHubAiConversationMessage, DataHubAiConversationMessageType } from '@types';

const ThinkingContainer = styled.div`
    display: flex;
    padding: 4px 24px 0px 24px;
    justify-content: flex-start;
`;

const ThinkingContent = styled.div`
    width: 100%;
    display: flex;
    flex-direction: column;
    align-items: flex-start;
`;

const ThinkingHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 6px;
    margin-bottom: 4px;
    width: 100%;
`;

const ThinkingLabelContent = styled.div`
    display: flex;
    align-items: center;
    gap: 6px;
    color: ${colors.gray[600]};
    font-size: 12px;
    font-weight: 400;
    padding: 4px 0;
`;

const LiveThinkingContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
    width: 100%;
    color: ${colors.gray[600]};
    font-size: 12px;
    line-height: 1.5;
`;

const LoaderWrapper = styled.div`
    flex-shrink: 0;
    display: flex;
    align-items: center;

    /* Scale down the loader to half size */
    & > * {
        transform: scale(0.5);
        transform-origin: center;
    }
`;

const TextWrapper = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    align-items: flex-start;
`;

const ThinkingBubble = styled.div`
    padding: 8px 0;
    max-height: 300px;
    overflow-y: auto;
    width: 100%;
    margin-bottom: 8px;

    /* Custom scrollbar */
    &::-webkit-scrollbar {
        width: 4px;
    }

    &::-webkit-scrollbar-track {
        background: transparent;
    }

    &::-webkit-scrollbar-thumb {
        background: ${colors.gray[300]};
        border-radius: 2px;
    }

    &::-webkit-scrollbar-thumb:hover {
        background: ${colors.gray[400]};
    }
`;

const ThinkingMessageBlock = styled.div`
    font-size: 12px;
    line-height: 1.5;
    color: ${colors.gray[600]};
    margin-bottom: 12px;

    &:last-child {
        margin-bottom: 0;
    }
`;

const MarkdownContent = styled.div`
    color: ${colors.gray[600]} !important;
    font-size: 12px !important;
    line-height: 1.5;

    & * {
        color: ${colors.gray[600]} !important;
        font-size: 12px !important;
    }

    & p {
        margin: 0 0 8px 0;
        color: ${colors.gray[600]} !important;
    }

    & p:last-child {
        margin-bottom: 0;
    }

    & div,
    & span {
        color: ${colors.gray[600]} !important;
    }

    & .wmde-markdown,
    & .wmde-markdown * {
        color: ${colors.gray[600]} !important;
        font-size: 12px !important;
    }

    & code {
        background-color: rgba(0, 0, 0, 0.06);
        padding: 2px 6px;
        border-radius: 4px;
        font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
        font-size: 11px !important;
        color: ${colors.gray[600]} !important;
    }

    & pre {
        background-color: #f6f8fa;
        padding: 8px;
        border-radius: 6px;
        overflow-x: auto;
        margin: 4px 0;
    }

    & pre code {
        background-color: transparent;
        padding: 0;
    }
`;

interface ThinkingGroupProps {
    messages: DataHubAiConversationMessage[];
    verboseMode?: boolean;
    isComplete?: boolean;
}

export const ThinkingGroup: React.FC<ThinkingGroupProps> = ({ messages, verboseMode, isComplete }) => {
    const bubbleRef = useRef<HTMLDivElement>(null);
    const [isExpanded, setIsExpanded] = useState(false);

    // Calculate duration if complete
    const getDuration = () => {
        if (!isComplete || messages.length === 0) return null;

        const firstTime = messages[0].time;
        const lastTime = messages[messages.length - 1].time;
        const durationMs = lastTime - firstTime;
        const durationSeconds = Math.round(durationMs / 1000);

        return durationSeconds;
    };

    const duration = getDuration();

    // Auto-collapse when complete
    useEffect(() => {
        if (isComplete) {
            setIsExpanded(false);
        }
    }, [isComplete]);

    // Auto-scroll to bottom when new messages arrive (only if expanded or not complete)
    useEffect(() => {
        if (bubbleRef.current && (isExpanded || !isComplete)) {
            bubbleRef.current.scrollTop = bubbleRef.current.scrollHeight;
        }
    }, [messages, isExpanded, isComplete]);

    // Get only thinking messages (filter out tool calls/results unless in verbose mode)
    const thinkingMessages = messages.filter((msg) => {
        if (msg.type === DataHubAiConversationMessageType.Thinking) {
            return true;
        }
        // Only include tool messages in verbose mode
        if (
            verboseMode &&
            (msg.type === DataHubAiConversationMessageType.ToolCall ||
                msg.type === DataHubAiConversationMessageType.ToolResult)
        ) {
            return true;
        }
        return false;
    });

    const lastThinkingMessage = thinkingMessages.length > 0 ? thinkingMessages[thinkingMessages.length - 1] : null;

    const renderLiveThinking = () => {
        return (
            <LiveThinkingContainer>
                <LoaderWrapper>
                    <Loader size="sm" />
                </LoaderWrapper>
                <TextWrapper>
                    {lastThinkingMessage ? (
                        <MarkdownContent>
                            <MDEditor.Markdown
                                source={lastThinkingMessage.content.text}
                                style={{ backgroundColor: 'transparent', color: 'inherit' }}
                            />
                        </MarkdownContent>
                    ) : (
                        <span>Thinking...</span>
                    )}
                </TextWrapper>
            </LiveThinkingContainer>
        );
    };

    const renderCompletedThinking = () => {
        return (
            <ThinkingBubble ref={bubbleRef}>
                {thinkingMessages.map((message) => (
                    <ThinkingMessageBlock key={message.time}>
                        <MarkdownContent>
                            <MDEditor.Markdown
                                source={message.content.text}
                                style={{ backgroundColor: 'transparent', color: 'inherit' }}
                            />
                        </MarkdownContent>
                    </ThinkingMessageBlock>
                ))}
            </ThinkingBubble>
        );
    };

    const showExpandedContent = isComplete && isExpanded;

    const renderLabel = () => {
        const labelText =
            isComplete && duration !== null
                ? `Thought for ${duration} second${duration !== 1 ? 's' : ''}`
                : 'Thinking...';

        const labelContent = (
            <ThinkingLabelContent>
                <Lightning size={12} weight="fill" />
                <span>{labelText}</span>
            </ThinkingLabelContent>
        );

        // If complete, make it a clickable button
        if (isComplete) {
            return (
                <Button
                    variant="text"
                    size="sm"
                    onClick={() => setIsExpanded(!isExpanded)}
                    style={{ padding: '0', minHeight: 'auto', color: colors.gray[600] }}
                >
                    {labelContent}
                </Button>
            );
        }

        // Otherwise just show the label
        return labelContent;
    };

    return (
        <ThinkingContainer>
            <ThinkingContent>
                {!isComplete ? (
                    // Live thinking - show spinner with last message
                    renderLiveThinking()
                ) : (
                    // Completed thinking - show collapsible header and content
                    <>
                        <ThinkingHeader>
                            {renderLabel()}
                            {isExpanded && (
                                <Button variant="text" size="sm" color="gray" onClick={() => setIsExpanded(false)}>
                                    Hide
                                </Button>
                            )}
                        </ThinkingHeader>
                        {showExpandedContent && renderCompletedThinking()}
                    </>
                )}
            </ThinkingContent>
        </ThinkingContainer>
    );
};
