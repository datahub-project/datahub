import { Button, colors } from '@components';
import { Lightning } from '@phosphor-icons/react';
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

const ThinkingBubble = styled.div`
    background-color: ${colors.gray[100]};
    padding: 8px 12px;
    border-radius: 8px;
    max-height: 80px;
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

const ThinkingList = styled.ul`
    margin: 0;
    padding: 0 0 0 20px;
    list-style: disc;
`;

const ThinkingItem = styled.li`
    font-size: 13px;
    line-height: 1.5;
    color: ${colors.gray[700]};
    margin-bottom: 4px;

    &:last-child {
        margin-bottom: 0;
    }
`;

const ToolItem = styled.ul`
    margin: 4px 0 0 0;
    padding: 0 0 0 20px;
    list-style: circle;
`;

const ToolSubItem = styled.li`
    font-size: 12px;
    line-height: 1.4;
    color: ${colors.gray[600]};
    margin-bottom: 2px;
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

    const renderMessageContent = (message: DataHubAiConversationMessage) => {
        const { text } = message.content;

        if (message.type === DataHubAiConversationMessageType.Thinking) {
            return <ThinkingItem key={message.time}>{text}</ThinkingItem>;
        }

        // Only show tool calls/results in verbose mode
        if (!verboseMode) {
            return null;
        }

        if (message.type === DataHubAiConversationMessageType.ToolCall) {
            return (
                <ToolItem key={message.time}>
                    <ToolSubItem>
                        <strong>Tool:</strong> {text.substring(0, 50)}
                        {text.length > 50 && '...'}
                    </ToolSubItem>
                </ToolItem>
            );
        }

        if (message.type === DataHubAiConversationMessageType.ToolResult) {
            return (
                <ToolItem key={message.time}>
                    <ToolSubItem>
                        <strong>Result:</strong> {text.substring(0, 50)}
                        {text.length > 50 && '...'}
                    </ToolSubItem>
                </ToolItem>
            );
        }

        return null;
    };

    const showBubble = !isComplete || isExpanded;

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
                <ThinkingHeader>
                    {renderLabel()}
                    {isComplete && isExpanded && (
                        <Button variant="text" size="sm" color="gray" onClick={() => setIsExpanded(false)}>
                            Hide
                        </Button>
                    )}
                </ThinkingHeader>
                {showBubble && (
                    <ThinkingBubble ref={bubbleRef}>
                        <ThinkingList>{messages.map((message) => renderMessageContent(message))}</ThinkingList>
                    </ThinkingBubble>
                )}
            </ThinkingContent>
        </ThinkingContainer>
    );
};
