import { useCallback, useRef, useState } from 'react';

import analytics, { EventType as AnalyticsEventType } from '@app/analytics';

import { DataHubAiConversationActorType, DataHubAiConversationMessage, DataHubAiConversationMessageType } from '@types';

export interface MessageContext {
    text: string;
    entityUrns?: string[];
}

interface StreamState {
    isStreaming: boolean;
    currentMessage: DataHubAiConversationMessage | null;
    error: string | null;
}

interface UseChatStreamProps {
    conversationUrn: string;
    onMessageReceived?: (message: DataHubAiConversationMessage) => void;
    onStreamComplete?: () => void;
    agentName?: string;
}

const processSSELine = (
    line: string,
    currentStreamingMessage: DataHubAiConversationMessage | null,
    onMessageReceived?: (message: DataHubAiConversationMessage) => void,
): {
    message: DataHubAiConversationMessage | null;
    shouldComplete: boolean;
    hasNewCompleteMessage: boolean;
    hasError: boolean;
    eventType?: string;
} => {
    if (!line.trim()) {
        return {
            message: currentStreamingMessage,
            shouldComplete: false,
            hasNewCompleteMessage: false,
            hasError: false,
        };
    }

    // Track event type from "event:" lines
    if (line.startsWith('event:')) {
        const eventType = line.substring(6).trim(); // Remove "event:" prefix

        // Check for error or complete events
        if (eventType === 'error') {
            console.error('=== SSE ERROR EVENT ===');
            console.error('Received error event from server');
            console.error('======================');
            return {
                message: currentStreamingMessage,
                shouldComplete: true,
                hasNewCompleteMessage: false,
                hasError: true,
                eventType,
            };
        }

        if (eventType === 'complete') {
            return {
                message: currentStreamingMessage,
                shouldComplete: true,
                hasNewCompleteMessage: false,
                hasError: false,
                eventType,
            };
        }

        return {
            message: currentStreamingMessage,
            shouldComplete: false,
            hasNewCompleteMessage: false,
            hasError: false,
            eventType,
        };
    }

    if (!line.startsWith('data:')) {
        return {
            message: currentStreamingMessage,
            shouldComplete: false,
            hasNewCompleteMessage: false,
            hasError: false,
        };
    }

    const data = line.slice(5); // Remove 'data:' prefix

    if (data === '') {
        return {
            message: currentStreamingMessage,
            shouldComplete: false,
            hasNewCompleteMessage: false,
            hasError: false,
        };
    }

    try {
        const parsed = JSON.parse(data);

        // Check if this is an error message
        if (parsed.error) {
            console.error('=== SSE ERROR DATA ===');
            console.error('Received error in SSE data:', parsed.error);
            console.error('Full parsed data:', parsed);
            console.error('=====================');
            return {
                message: currentStreamingMessage,
                shouldComplete: true,
                hasNewCompleteMessage: false,
                hasError: true,
            };
        }

        // Parse message from SSE data - the message is nested under 'message' key
        const messageData = parsed.message;
        if (messageData.type && messageData.content) {
            const newMessage: DataHubAiConversationMessage = {
                type: messageData.type as DataHubAiConversationMessageType,
                time: messageData.time || Date.now(),
                actor: messageData.actor || { type: DataHubAiConversationActorType.Agent },
                content: {
                    text: messageData.content.text || '',
                },
            };

            // If it's a streaming text message from the agent, accumulate it
            if (
                newMessage.type === DataHubAiConversationMessageType.Text &&
                newMessage.actor.type === DataHubAiConversationActorType.Agent
            ) {
                const updatedMessage = currentStreamingMessage
                    ? {
                          ...currentStreamingMessage,
                          content: {
                              ...currentStreamingMessage.content,
                              text: currentStreamingMessage.content.text + newMessage.content.text,
                          },
                      }
                    : newMessage;
                return {
                    message: updatedMessage,
                    shouldComplete: false,
                    hasNewCompleteMessage: false,
                    hasError: false,
                };
            }

            // For non-text messages (thinking, tool calls, etc.), add to messages immediately
            // These are complete messages, so we should yield after processing
            if (onMessageReceived) {
                onMessageReceived(newMessage);
            }
            // Reset current streaming message since we got a non-text message
            return { message: null, shouldComplete: false, hasNewCompleteMessage: true, hasError: false };
        }
    } catch (e) {
        console.error('=== SSE PARSING ERROR ===');
        console.error('Failed to parse SSE line:', line);
        console.error('Parse error:', e);
        console.error('========================');
    }

    return { message: currentStreamingMessage, shouldComplete: false, hasNewCompleteMessage: false, hasError: false };
};

const readStream = async (
    reader: ReadableStreamDefaultReader<Uint8Array>,
    decoder: TextDecoder,
    onMessageReceived?: (message: DataHubAiConversationMessage) => void,
    onProgress?: (message: DataHubAiConversationMessage) => void,
): Promise<DataHubAiConversationMessage | null> => {
    let buffer = '';
    let currentStreamingMessage: DataHubAiConversationMessage | null = null;
    let shouldComplete = false;
    let hasError = false;

    // Use while loop instead of recursion to allow event loop to process
    // eslint-disable-next-line no-await-in-loop
    while (!shouldComplete) {
        let done;
        let value;

        try {
            // eslint-disable-next-line no-await-in-loop
            const result = await reader.read();
            done = result.done;
            value = result.value;
        } catch (error) {
            console.error('=== STREAM CONNECTION ERROR ===');
            console.error('Error reading from stream:', error);
            console.error('===============================');
            throw new Error('Connection interrupted');
        }

        if (done) {
            break;
        }

        buffer += decoder.decode(value, { stream: true });

        // Process buffer and notify progress for each line
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        let shouldYield = false;

        // eslint-disable-next-line no-restricted-syntax
        for (const line of lines) {
            const result = processSSELine(line, currentStreamingMessage, onMessageReceived);
            currentStreamingMessage = result.message;
            shouldComplete = result.shouldComplete || shouldComplete;
            hasError = result.hasError || hasError;

            // Track if we got a new complete message (thinking, tool call, tool result)
            if (result.hasNewCompleteMessage) {
                shouldYield = true;
            }

            // Notify progress for streaming text messages
            if (currentStreamingMessage && onProgress) {
                onProgress(currentStreamingMessage);
            }

            if (shouldComplete) {
                break;
            }
        }

        // Only yield if we received a complete new message (not just text chunks)
        // This allows all text chunks to accumulate without yielding, but yields
        // after each thinking/tool message to show them incrementally
        if (shouldYield) {
            // eslint-disable-next-line no-await-in-loop
            await new Promise((resolve) => {
                setTimeout(resolve, 10);
            });
        }
    }

    // If we encountered an error during streaming, throw
    if (hasError) {
        throw new Error('Stream error');
    }

    return currentStreamingMessage;
};

export const useChatStream = ({
    conversationUrn,
    onMessageReceived,
    onStreamComplete,
    agentName,
}: UseChatStreamProps) => {
    const [state, setState] = useState<StreamState>({
        isStreaming: false,
        currentMessage: null,
        error: null,
    });

    const abortControllerRef = useRef<AbortController | null>(null);
    const messageQueueRef = useRef<{ text: string; convoUrn?: string; messageContext?: MessageContext }[]>([]);
    const isProcessingRef = useRef(false);

    const cleanup = useCallback(() => {
        if (abortControllerRef.current) {
            abortControllerRef.current.abort();
            abortControllerRef.current = null;
        }
        setState((prev) => ({ ...prev, isStreaming: false, currentMessage: null }));
        isProcessingRef.current = false;
    }, []);

    const stopStreaming = useCallback(() => {
        cleanup();
    }, [cleanup]);

    const processNextMessage = useCallback(
        async (messageText: string, convoUrn?: string, messageContext?: MessageContext) => {
            setState({
                isStreaming: true,
                currentMessage: null,
                error: null,
            });

            try {
                abortControllerRef.current = new AbortController();

                const requestBody: {
                    conversationUrn: string;
                    text: string;
                    agentName?: string;
                    context?: MessageContext;
                } = {
                    conversationUrn: convoUrn || conversationUrn,
                    text: messageText,
                };

                if (agentName) {
                    requestBody.agentName = agentName;
                }

                if (messageContext) {
                    requestBody.context = messageContext;
                }

                const response = await fetch('/openapi/v1/ai-chat/message', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(requestBody),
                    signal: abortControllerRef.current.signal,
                });

                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }

                const reader = response.body?.getReader();
                const decoder = new TextDecoder();

                if (!reader) {
                    throw new Error('No reader available');
                }

                const handleProgress = (message: DataHubAiConversationMessage) => {
                    setState((prev) => ({
                        ...prev,
                        currentMessage: message,
                    }));
                };

                const finalMessage = await readStream(reader, decoder, onMessageReceived, handleProgress);

                if (finalMessage && onMessageReceived) {
                    onMessageReceived(finalMessage);
                }

                if (onStreamComplete) {
                    onStreamComplete();
                }

                setState({
                    isStreaming: false,
                    currentMessage: null,
                    error: null,
                });
            } catch (error: any) {
                // Silently handle AbortError and Connection interrupted
                // These indicate user intentionally stopped the stream
                const isExpectedDisconnect =
                    error.name === 'AbortError' || (error.message && error.message.includes('Connection interrupted'));

                if (isExpectedDisconnect) {
                    setState({
                        isStreaming: false,
                        currentMessage: null,
                        error: null,
                    });
                } else {
                    // Comprehensive error logging for debugging
                    console.error('=== CHAT STREAM ERROR! ===');
                    console.error('Error Type:', error.name);
                    console.error('Error Message:', error.message);
                    console.error('Error Stack:', error.stack);
                    console.error('Conversation URN:', conversationUrn);
                    console.error('Message Text:', messageText);
                    console.error('Full Error Object:', error);
                    console.error('========================');

                    // Emit analytics event for chat response error
                    analytics.event({
                        type: AnalyticsEventType.DataHubChatResponseErrorEvent,
                        conversationUrn,
                        errorMessage: error.message || 'Unknown error',
                        errorType: error.name || 'UnknownError',
                        statusCode: error.status || undefined,
                        messagePreview: messageText.substring(0, 200), // First 200 characters of message that caused error
                    });

                    // Create an error message to display in the chat
                    const errorMessage: DataHubAiConversationMessage = {
                        type: DataHubAiConversationMessageType.Text,
                        time: Date.now(),
                        actor: { type: DataHubAiConversationActorType.Agent },
                        content: {
                            text: 'Oops! An unexpected error occurred. 🥹 Please try again in a little while.',
                        },
                    };

                    // Send error message to UI
                    if (onMessageReceived) {
                        onMessageReceived(errorMessage);
                    }

                    setState({
                        isStreaming: false,
                        currentMessage: null,
                        error: error.message || 'Failed to send message',
                    });
                }
            }
        },
        [conversationUrn, onMessageReceived, onStreamComplete, agentName],
    );

    const sendMessage = useCallback(
        async (text: string, convoUrn?: string, messageContext?: MessageContext) => {
            messageQueueRef.current.push({ text, convoUrn, messageContext });

            if (isProcessingRef.current) {
                return;
            }

            isProcessingRef.current = true;

            const processQueue = async () => {
                const nextItem = messageQueueRef.current.shift();
                if (!nextItem) {
                    isProcessingRef.current = false;
                    return;
                }

                await processNextMessage(nextItem.text, nextItem.convoUrn, nextItem.messageContext);
                await processQueue();
            };

            await processQueue();
        },
        [processNextMessage],
    );

    return {
        sendMessage,
        stopStreaming,
        isStreaming: state.isStreaming,
        currentMessage: state.currentMessage,
        error: state.error,
    };
};
