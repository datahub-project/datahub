import { useState, useCallback, useRef, useEffect } from 'react';
import { apiClient } from '../api/client';
import type { Conversation, Message } from '../api/types';

export function useChat() {
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [currentConversation, setCurrentConversation] = useState<Conversation | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [streamingMessage, setStreamingMessage] = useState<string>('');
  const [isStreaming, setIsStreaming] = useState(false);

  // Track which conversation is currently being viewed (for checking in SSE callbacks)
  const viewingConversationIdRef = useRef<string | null>(currentConversation?.id || null);

  // Track streaming state PER conversation (multiple can stream simultaneously)
  const streamingStatesRef = useRef<Map<string, {
    isStreaming: boolean;
    accumulatedContent: string;
    events: string[];
    finalText: string;
    eventMetadata: Record<string, any>;
    thinkingStartTime: number | null;
  }>>(new Map());

  // Keep viewingConversationIdRef in sync with currentConversation
  useEffect(() => {
    viewingConversationIdRef.current = currentConversation?.id || null;
  }, [currentConversation]);

  // Helper: Get or initialize streaming state for a conversation
  const getStreamingState = useCallback((conversationId: string) => {
    if (!streamingStatesRef.current.has(conversationId)) {
      streamingStatesRef.current.set(conversationId, {
        isStreaming: false,
        accumulatedContent: '',
        events: [],
        finalText: '',
        eventMetadata: {},
        thinkingStartTime: null,
      });
    }
    return streamingStatesRef.current.get(conversationId)!;
  }, []);

  const loadConversations = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await apiClient.getConversations();
      setConversations(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load conversations');
    } finally {
      setLoading(false);
    }
  }, []);

  const createConversation = useCallback(async (title?: string) => {
    try {
      setLoading(true);
      setError(null);

      console.log('[createConversation] Creating new conversation with title:', title);
      console.log('[createConversation] Current conversation before create:', currentConversation?.id);

      // Clear streaming UI state when creating a new conversation
      // But DON'T clear streamingConversationIdRef - let background streaming continue
      setStreamingMessage('');
      setIsStreaming(false);

      const conversation = await apiClient.createConversation(title);
      console.log('[createConversation] API returned conversation:', conversation.id, 'with', conversation.messages.length, 'messages');
      console.log('[createConversation] Messages:', JSON.stringify(conversation.messages));

      setConversations((prev) => [conversation, ...prev]);
      setCurrentConversation(conversation);

      // Update viewingConversationIdRef synchronously so immediate sendMessage calls work correctly
      viewingConversationIdRef.current = conversation.id;

      console.log('[createConversation] Set as current conversation');
      return conversation;
    } catch (err) {
      console.error('[createConversation] Error creating conversation:', err);
      setError(err instanceof Error ? err.message : 'Failed to create conversation');
      throw err;
    } finally {
      setLoading(false);
    }
  }, [currentConversation]);

  const loadConversation = useCallback(async (id: string) => {
    console.log(`[loadConversation] Starting to load conversation: ${id}`);

    // First, find the conversation in the conversations array and set it immediately
    // This ensures we switch the view right away, even before the API call
    const cachedConversation = conversations.find(c => c.id === id);
    if (cachedConversation) {
      console.log(`[loadConversation] Found cached conversation with ${cachedConversation.messages.length} messages, switching immediately`);
      setCurrentConversation(cachedConversation);

      // Update viewingConversationIdRef synchronously
      viewingConversationIdRef.current = id;

      // Check if this conversation has streaming state
      const streamState = getStreamingState(id);

      if (streamState.isStreaming) {
        console.log(`[loadConversation] Restoring streaming state for conversation ${id}`);
        setIsStreaming(true);
        setStreamingMessage(streamState.accumulatedContent);
      } else {
        console.log(`[loadConversation] Not streaming - clearing streaming UI`);
        setStreamingMessage('');
        setIsStreaming(false);
      }
    }

    try {
      setLoading(true);
      setError(null);

      console.log('[loadConversation] Fetching latest conversation data from API');
      console.log(`[loadConversation] Calling apiClient.getConversation(${id})`);
      const conversation = await apiClient.getConversation(id);
      console.log(`[loadConversation] API call completed`);
      console.log(`[loadConversation] Fetched conversation: ${conversation.id} with ${conversation.messages.length} messages`);

      console.log('[loadConversation] Updating with fresh data from API');

      // Update BOTH conversations array (cache) and currentConversation
      // This ensures cache is always fresh and prevents stale data
      setConversations((prevConvs) =>
        prevConvs.map((conv) => (conv.id === id ? conversation : conv))
      );
      setCurrentConversation(conversation);
      console.log('[loadConversation] Updated conversations array and current conversation');
    } catch (err) {
      console.error('[loadConversation] Error:', err);
      setError(err instanceof Error ? err.message : 'Failed to load conversation');
    } finally {
      setLoading(false);
      console.log('[loadConversation] Completed');
    }
  }, [conversations, getStreamingState]);

  const deleteConversation = useCallback(async (id: string) => {
    try {
      setLoading(true);
      setError(null);
      await apiClient.deleteConversation(id);

      // Remove from conversations array
      const remainingConversations = conversations.filter((c) => c.id !== id);
      setConversations(remainingConversations);

      // If we deleted the currently viewing conversation, switch to another
      if (currentConversation?.id === id) {
        if (remainingConversations.length > 0) {
          // Switch to the first remaining conversation
          setCurrentConversation(remainingConversations[0]);
        } else {
          // No conversations left
          setCurrentConversation(null);
        }
      }

      // Clean up streaming state for deleted conversation
      streamingStatesRef.current.delete(id);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to delete conversation');
    } finally {
      setLoading(false);
    }
  }, [currentConversation, conversations]);

  const sendMessage = useCallback(
    async (content: string, conversationId?: string) => {
      // Use provided conversationId or fall back to currentConversation
      const sendingConversationId = conversationId || currentConversation?.id;

      if (!sendingConversationId) {
        throw new Error('No conversation selected');
      }

      // Generate truly unique ID using conversation ID + timestamp + random
      const userMessage: Message = {
        id: `temp-${sendingConversationId}-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`,
        role: 'user',
        content,
        timestamp: new Date().toISOString(),
      };

      // Update BOTH conversations array and currentConversation
      // This ensures when you switch back, the cached version has the user message
      setConversations((prevConvs) =>
        prevConvs.map((conv) =>
          conv.id === sendingConversationId
            ? { ...conv, messages: [...conv.messages, userMessage] }
            : conv
        )
      );

      setCurrentConversation((prev) => {
        if (!prev || prev.id !== sendingConversationId) {
          console.log(`[sendMessage] NOT adding user message - viewing different conversation`);
          return prev;
        }
        console.log(`[sendMessage] Adding user message to currentConversation ${sendingConversationId}`);
        return {
          ...prev,
          messages: [...prev.messages, userMessage],
        };
      });

      setStreamingMessage('');
      setError(null);

      // Only set global streaming state if we're viewing this conversation
      if (viewingConversationIdRef.current === sendingConversationId) {
        setIsStreaming(true);
      }

      // Get or create streaming state for this conversation
      const streamState = getStreamingState(sendingConversationId);
      streamState.isStreaming = true;

      console.log(`[sendMessage] Starting stream for conversation ${sendingConversationId}, currently viewing: ${viewingConversationIdRef.current}`);
      streamState.accumulatedContent = '';
      streamState.events = [];
      streamState.finalText = '';
      streamState.eventMetadata = {};
      streamState.thinkingStartTime = null;

      await apiClient.sendMessage(
        sendingConversationId,
        { content },
        (data) => {
          // Always process SSE events and update the per-conversation streaming state
          // Only update UI if we're currently viewing this conversation

          const streamState = getStreamingState(sendingConversationId);

          // Handle SSE message based on type
          if (data.type === 'THINKING') {
            // Track start time for wall clock calculation
            if (streamState.thinkingStartTime === null) {
              streamState.thinkingStartTime = Date.now();
            }

            // Add thinking block in order with metadata
            const thinkingText = data.content?.text || 'Thinking...';
            const tokens = data.content?.tokens || 0;
            const eventId = data.content?.eventId || `thinking-${Date.now()}`;
            const wallClockTime = (Date.now() - streamState.thinkingStartTime) / 1000;

            // Store metadata
            streamState.eventMetadata[eventId] = { tokens, type: 'thinking', wallClockTime };
            streamState.eventMetadata.__wallClockTime = wallClockTime;

            // Build XML with metadata attributes
            let thinkingXml = `<thinking eventId="${eventId}" tokens="${tokens}" wallClockTime="${wallClockTime.toFixed(2)}">`;
            thinkingXml += thinkingText;
            thinkingXml += `</thinking>`;
            streamState.events.push(thinkingXml);

            // Build current accumulated content
            streamState.accumulatedContent = streamState.events.join('\n\n') + (streamState.finalText ? '\n\n' + streamState.finalText : '');

            // Only update UI if we're viewing this conversation
            if (viewingConversationIdRef.current === sendingConversationId) {
              setStreamingMessage(streamState.accumulatedContent);
            }

          } else if (data.type === 'THINKING_COMPLETE') {
            // Update metadata with duration
            const eventId = data.content?.eventId;
            const duration = data.content?.duration;
            if (eventId && streamState.eventMetadata[eventId]) {
              streamState.eventMetadata[eventId].duration = duration;

              // Update the XML in events array
              const eventIndex = streamState.events.findIndex((e) => e.includes(`eventId="${eventId}"`));
              if (eventIndex >= 0) {
                const oldXml = streamState.events[eventIndex];
                // Add duration attribute
                const newXml = oldXml.replace(
                  `tokens="${streamState.eventMetadata[eventId].tokens}">`,
                  `tokens="${streamState.eventMetadata[eventId].tokens}" duration="${duration.toFixed(2)}">`,
                );
                streamState.events[eventIndex] = newXml;

                // Rebuild content
                streamState.accumulatedContent = streamState.events.join('\n\n') + (streamState.finalText ? '\n\n' + streamState.finalText : '');

                // Only update UI if viewing this conversation
                if (viewingConversationIdRef.current === sendingConversationId) {
                  setStreamingMessage(streamState.accumulatedContent);
                }
              }
            }

          } else if (data.type === 'TOOL_CALL') {
            // Track start time for wall clock calculation
            if (streamState.thinkingStartTime === null) {
              streamState.thinkingStartTime = Date.now();
            }

            // Add tool call in order with metadata
            const toolName = data.content?.toolName || 'unknown';
            const toolInput = data.content?.toolInput || {};
            const tokens = data.content?.tokens || 0;
            const eventId = data.content?.eventId || `tool-${Date.now()}`;
            const wallClockTime = (Date.now() - streamState.thinkingStartTime) / 1000;

            // Store metadata
            streamState.eventMetadata[eventId] = { tokens, type: 'tool', toolName, wallClockTime };
            streamState.eventMetadata.__wallClockTime = wallClockTime;

            // Calculate total tokens so far
            let totalTokens = 0;
            Object.keys(streamState.eventMetadata).forEach(key => {
              if (key !== '__wallClockTime' && streamState.eventMetadata[key].tokens) {
                totalTokens += streamState.eventMetadata[key].tokens;
                if (streamState.eventMetadata[key].resultTokens) {
                  totalTokens += streamState.eventMetadata[key].resultTokens;
                }
              }
            });
            streamState.eventMetadata.__totalTokens = totalTokens;

            // Build tool_use XML with metadata and parameters
            let toolXml = `<tool_use eventId="${eventId}" tokens="${tokens}" wallClockTime="${wallClockTime.toFixed(2)}">\n`;
            toolXml += `<tool_name>${toolName}</tool_name>\n`;
            if (Object.keys(toolInput).length > 0) {
              toolXml += `<parameters>${JSON.stringify(toolInput, null, 2)}</parameters>\n`;
            }
            toolXml += `</tool_use>`;
            streamState.events.push(toolXml);

            // Build current accumulated content
            streamState.accumulatedContent = streamState.events.join('\n\n') + (streamState.finalText ? '\n\n' + streamState.finalText : '');

            // Only update UI if viewing this conversation
            if (viewingConversationIdRef.current === sendingConversationId) {
              setStreamingMessage(streamState.accumulatedContent);
            }

          } else if (data.type === 'TOOL_COMPLETE') {
            // Update metadata with duration and result tokens
            const eventId = data.content?.eventId;
            const duration = data.content?.duration;
            const resultTokens = data.content?.resultTokens || 0;
            if (eventId && streamState.eventMetadata[eventId]) {
              streamState.eventMetadata[eventId].duration = duration;
              streamState.eventMetadata[eventId].resultTokens = resultTokens;

              // Recalculate total tokens including result tokens
              let totalTokens = 0;
              Object.keys(streamState.eventMetadata).forEach(key => {
                if (key !== '__wallClockTime' && key !== '__totalTokens' && streamState.eventMetadata[key].tokens) {
                  totalTokens += streamState.eventMetadata[key].tokens;
                  if (streamState.eventMetadata[key].resultTokens) {
                    totalTokens += streamState.eventMetadata[key].resultTokens;
                  }
                }
              });
              streamState.eventMetadata.__totalTokens = totalTokens;

              // Update the XML in events array
              const eventIndex = streamState.events.findIndex((e) => e.includes(`eventId="${eventId}"`));
              if (eventIndex >= 0) {
                const oldXml = streamState.events[eventIndex];
                // Add duration and resultTokens attributes
                const newXml = oldXml.replace(
                  `wallClockTime="${streamState.eventMetadata[eventId].wallClockTime.toFixed(2)}">`,
                  `wallClockTime="${streamState.eventMetadata[eventId].wallClockTime.toFixed(2)}" duration="${duration.toFixed(2)}" resultTokens="${resultTokens}">`,
                );
                streamState.events[eventIndex] = newXml;

                // Rebuild content
                streamState.accumulatedContent = streamState.events.join('\n\n') + (streamState.finalText ? '\n\n' + streamState.finalText : '');

                // Only update UI if viewing this conversation
                if (viewingConversationIdRef.current === sendingConversationId) {
                  setStreamingMessage(streamState.accumulatedContent);
                }
              }
            }

          } else if (data.type === 'TEXT') {
            // This is the final response - accumulate it
            streamState.finalText += data.content?.text || '';

            // Build full content with all parts
            streamState.accumulatedContent = streamState.events.join('\n\n') + (streamState.finalText ? '\n\n' + streamState.finalText : '');

            // Only update UI if viewing this conversation
            if (viewingConversationIdRef.current === sendingConversationId) {
              setStreamingMessage(streamState.accumulatedContent);
            }

          } else if (data.type === 'done') {
            // Don't add assistant message optimistically - let backend refetch handle it
            // This prevents duplicate messages when switching conversations rapidly

            // Clear streaming state for this conversation
            streamState.isStreaming = false;

            // Only clear streaming UI if we're viewing this conversation
            if (viewingConversationIdRef.current === sendingConversationId) {
              setStreamingMessage('');
            }
          }
        },
        (errorMsg) => {
          // Clear streaming state for this conversation
          const streamState = getStreamingState(sendingConversationId);
          streamState.isStreaming = false;

          // Only update error UI if we're viewing this conversation
          if (viewingConversationIdRef.current === sendingConversationId) {
            setError(errorMsg);
            setStreamingMessage('');
            setIsStreaming(false);
          }
        },
        () => {
          // Done callback - clear streaming state for this conversation
          const streamState = getStreamingState(sendingConversationId);
          streamState.isStreaming = false;

          // Only update UI if we're viewing this conversation
          if (viewingConversationIdRef.current === sendingConversationId) {
            setIsStreaming(false);
          }

          // Refetch the full conversation from backend to get complete state
          // This ensures we have the correct messages and updated title
          console.log('[sendMessage] Done callback: Refetching conversation from backend');
          apiClient.getConversation(sendingConversationId).then((updatedConv) => {
            console.log(`[sendMessage] Got updated conversation: ${updatedConv.title} with ${updatedConv.messages.length} messages`);

            // Update conversations array with the fresh data
            console.log(`[sendMessage] Updating conversations array: conv ${sendingConversationId} now has ${updatedConv.messages.length} messages`);
            setConversations((prevConvs) =>
              prevConvs.map((conv) => {
                if (conv.id === sendingConversationId) {
                  console.log(`[sendMessage] Replacing cached conv (${conv.messages.length} msgs) with fresh data (${updatedConv.messages.length} msgs)`);
                  return updatedConv;
                }
                return conv;
              })
            );

            // If we're viewing this conversation, also update currentConversation
            // This ensures we have the backend's version with all messages
            if (viewingConversationIdRef.current === sendingConversationId) {
              console.log(`[sendMessage] Also updating currentConversation since we're viewing it`);
              setCurrentConversation(updatedConv);
            } else {
              console.log(`[sendMessage] NOT updating currentConversation (viewing ${viewingConversationIdRef.current}, not ${sendingConversationId})`);
            }
          }).catch((err) => {
            console.error('Failed to refetch conversation:', err);
          });
        }
      );
    },
    [currentConversation, getStreamingState]
  );

  return {
    conversations,
    currentConversation,
    loading,
    error,
    isStreaming,
    streamingMessage,
    loadConversations,
    createConversation,
    loadConversation,
    deleteConversation,
    sendMessage,
  };
}
