import { useState, useEffect, useRef } from 'react';
import type { ConnectionConfig, AwsHealthStatus } from '../api/types';
import { apiClient } from '../api/client';
import { JsonViewer } from './JsonViewer';

interface AutoChatBarProps {
  onSendMessage: (content: string, conversationId?: string) => Promise<void>;
  onCreateConversation: (title?: string) => Promise<{ id: string }>;
}

export function AutoChatBar({ onSendMessage, onCreateConversation }: AutoChatBarProps) {
  const [expanded, setExpanded] = useState(true);
  const [enabled, setEnabled] = useState(false);
  const [paused, setPaused] = useState(false);
  const [maxMessagesPerConv, setMaxMessagesPerConv] = useState(5);
  const [maxConversations, setMaxConversations] = useState(2);

  // Display-only state for UI
  const [currentConversationCount, setCurrentConversationCount] = useState(0);
  const [currentMessageCount, setCurrentMessageCount] = useState(0);
  const [totalMessagesSent, setTotalMessagesSent] = useState(0);

  const [config, setConfig] = useState<ConnectionConfig | null>(null);
  const [awsHealth, setAwsHealth] = useState<AwsHealthStatus | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Sketch modal state
  const [showSketchModal, setShowSketchModal] = useState(false);
  const [sketch, setSketch] = useState<any>(null);
  const [loadingSketch, setLoadingSketch] = useState(false);
  const [sketchError, setSketchError] = useState<string | null>(null);

  // Track current conversation and counters for auto-chat loop (refs to avoid stale closures)
  const currentConversationIdRef = useRef<string | null>(null);
  const isRunningRef = useRef(false);
  const currentConversationCountRef = useRef(0);
  const currentMessageCountRef = useRef(0);
  const totalMessagesSentRef = useRef(0);

  const fetchConfig = async () => {
    try {
      const cfg = await apiClient.getConfig();
      setConfig(cfg);
    } catch (err) {
      console.error('Failed to fetch config:', err);
    }
  };

  const checkAwsHealth = async () => {
    try {
      const health = await apiClient.checkAwsHealth();
      setAwsHealth(health);
    } catch (err) {
      console.error('Failed to check AWS health:', err);
      setAwsHealth({
        status: 'error',
        profile: 'unknown',
        message: 'Failed to check AWS health',
        details: { error: String(err) }
      });
    }
  };

  const handleShowSketch = async (e: React.MouseEvent) => {
    e.stopPropagation(); // Prevent header click from toggling expand
    setShowSketchModal(true);
    setLoadingSketch(true);
    setSketchError(null);

    try {
      const sketchData = await apiClient.getSketch();
      setSketch(sketchData);
    } catch (err) {
      console.error('Failed to fetch sketch:', err);
      setSketchError(err instanceof Error ? err.message : 'Failed to load sketch');
    } finally {
      setLoadingSketch(false);
    }
  };

  // Fetch config and AWS health on mount
  useEffect(() => {
    fetchConfig();
    checkAwsHealth();

    // Check AWS health every 60 seconds
    const healthInterval = setInterval(() => {
      checkAwsHealth();
    }, 60000);

    return () => {
      clearInterval(healthInterval);
    };
  }, []);

  // Auto-chat loop
  const runAutoChatLoop = async () => {
    if (!isRunningRef.current || paused) {
      return;
    }

    try {
      // Check if we need to rotate to a new conversation (use ref for accurate count)
      // Only rotate if we have an existing conversation AND we've reached the message limit
      if (currentConversationIdRef.current && currentMessageCountRef.current >= maxMessagesPerConv) {
        console.log(`Auto-chat: Message limit reached (${currentMessageCountRef.current}/${maxMessagesPerConv})`);

        // Check if we can create another conversation
        if (currentConversationCountRef.current >= maxConversations) {
          console.log('Auto-chat: Reached max conversations, stopping');
          setEnabled(false);
          isRunningRef.current = false;
          return;
        }

        console.log(`Auto-chat: Rotating to new conversation`);
        const newConv = await onCreateConversation(); // Let backend set title from first message
        currentConversationIdRef.current = newConv.id;

        // Update refs
        currentConversationCountRef.current += 1;
        currentMessageCountRef.current = 0;

        // Update state for display
        setCurrentConversationCount(currentConversationCountRef.current);
        setCurrentMessageCount(0);

        console.log(`Auto-chat: Now on conversation ${currentConversationCountRef.current}/${maxConversations}`);
      }

      // Generate question
      console.log('Auto-chat: Generating question...');
      const result = await apiClient.generateQuestion({
        aws_profile: config?.aws_profile || undefined,
      });
      console.log('Auto-chat: Question generated successfully');

      if (!result.success || !result.question) {
        console.error('Auto-chat: Failed to generate question:', result.error);
        throw new Error(result.error || 'Failed to generate question');
      }

      console.log('Auto-chat: Got question:', result.question);

      // Create conversation if needed (first iteration)
      if (!currentConversationIdRef.current) {
        console.log('Auto-chat: No conversation exists, creating new one...');
        const newConv = await onCreateConversation(); // Let backend set title from first message
        console.log('Auto-chat: Created conversation:', newConv.id);
        currentConversationIdRef.current = newConv.id;

        // Update refs
        currentConversationCountRef.current = 1;

        // Update state for display
        setCurrentConversationCount(1);
        console.log('Auto-chat: Set conversation count to 1');
      } else {
        console.log('Auto-chat: Using existing conversation:', currentConversationIdRef.current);
      }

      // Send the question as a normal message (pass conversation ID explicitly)
      console.log('Auto-chat: Sending question to conversation:', currentConversationIdRef.current);

      // sendMessage returns a promise that resolves when streaming completes
      await onSendMessage(result.question, currentConversationIdRef.current);

      console.log('Auto-chat: Message sent and streaming completed');

      // Update counters (refs first, then state for display)
      currentMessageCountRef.current += 1;
      totalMessagesSentRef.current += 1;

      setCurrentMessageCount(currentMessageCountRef.current);
      setTotalMessagesSent(totalMessagesSentRef.current);

      console.log(`Auto-chat: Updated counters - Msg ${currentMessageCountRef.current}/${maxMessagesPerConv}, Conv ${currentConversationCountRef.current}/${maxConversations}, Total: ${totalMessagesSentRef.current}`);

      // Small delay before next message
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Continue loop
      console.log('Auto-chat: Continuing to next iteration');
      runAutoChatLoop();

    } catch (err) {
      console.error('Auto-chat error:', err);
      setError(err instanceof Error ? err.message : 'Auto-chat error');
      setEnabled(false);
      isRunningRef.current = false;
    }
  };

  const handleStart = async () => {
    setError(null);
    try {
      // Reset refs (source of truth)
      currentConversationCountRef.current = 0;
      currentMessageCountRef.current = 0;
      totalMessagesSentRef.current = 0;
      currentConversationIdRef.current = null;

      // Reset state for display
      setCurrentConversationCount(0);
      setCurrentMessageCount(0);
      setTotalMessagesSent(0);

      // Enable and start loop
      setEnabled(true);
      setPaused(false);
      isRunningRef.current = true;

      console.log('Auto-chat: Starting with', maxMessagesPerConv, 'msgs/conv,', maxConversations, 'convos');

      // Start the loop
      runAutoChatLoop();

    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to start auto-chat');
      setEnabled(false);
      isRunningRef.current = false;
    }
  };

  const handlePause = () => {
    setPaused(true);
  };

  const handleResume = () => {
    setPaused(false);
    // Restart the loop if we're still enabled
    if (enabled && isRunningRef.current) {
      runAutoChatLoop();
    }
  };

  const handleStop = () => {
    setEnabled(false);
    setPaused(false);
    isRunningRef.current = false;
  };

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      isRunningRef.current = false;
    };
  }, []);

  const isRunning = enabled && !paused;
  const isPausedState = enabled && paused;
  const isIdle = !enabled;

  return (
    <div className="auto-chat-bar">
      <div className="auto-chat-header" onClick={() => setExpanded(!expanded)}>
        <span className="expand-icon">{expanded ? '▼' : '▶'}</span>
        <span className="auto-chat-title">Auto-Chat</span>
        {!expanded && enabled && (
          <span className="auto-chat-mini-status">
            {isPausedState ? '⏸' : '▶'} {paused ? 'Paused' : 'Running'} | Conv {currentConversationCount}/{maxConversations}
          </span>
        )}
        <button
          className="sketch-info-btn"
          onClick={handleShowSketch}
          title="View DataHub Sketch"
        >
          📋
        </button>
      </div>

      {expanded && (
        <div className="auto-chat-controls">
          {isIdle && (
            <>
              <div className="control-group">
                <label>Msgs/Conv:</label>
                <select
                  value={maxMessagesPerConv}
                  onChange={(e) => setMaxMessagesPerConv(parseInt(e.target.value))}
                  className="compact-select"
                >
                  {[1, 2, 3, 5, 10, 15, 20].map(n => (
                    <option key={n} value={n}>{n}</option>
                  ))}
                </select>
              </div>

              <div className="control-group">
                <label>Convos:</label>
                <select
                  value={maxConversations}
                  onChange={(e) => setMaxConversations(parseInt(e.target.value))}
                  className="compact-select"
                >
                  {[1, 2, 3, 5, 10].map(n => (
                    <option key={n} value={n}>{n}</option>
                  ))}
                </select>
              </div>

              <div className="control-group aws-profile-display">
                <label>AWS:</label>
                <span className="aws-profile-value">
                  {config?.aws_profile || 'default'}
                </span>
                {awsHealth && (
                  <span
                    className={`aws-health-indicator ${awsHealth.status}`}
                    title={awsHealth.message}
                  >
                    {awsHealth.status === 'healthy' && '🟢'}
                    {awsHealth.status === 'warning' && '🟡'}
                    {awsHealth.status === 'error' && '🔴'}
                    {awsHealth.status === 'unknown' && '⚪'}
                  </span>
                )}
              </div>

              <button onClick={handleStart} className="control-btn start-btn">
                ▶ Start
              </button>
            </>
          )}

          {(isRunning || isPausedState) && (
            <>
              <div className="status-group">
                <span className="status-indicator">
                  {isPausedState ? '⏸ Paused' : '▶ Running'}
                </span>
                <span className="status-item">
                  Conv {currentConversationCount}/{maxConversations}
                </span>
                <span className="status-item">
                  Msg {currentMessageCount}/{maxMessagesPerConv}
                </span>
                <span className="status-item">
                  {totalMessagesSent} total
                </span>
              </div>

              {isPausedState ? (
                <button onClick={handleResume} className="control-btn resume-btn">
                  ▶ Resume
                </button>
              ) : (
                <button onClick={handlePause} className="control-btn pause-btn">
                  ⏸ Pause
                </button>
              )}

              <button onClick={handleStop} className="control-btn stop-btn">
                ⏹ Stop
              </button>
            </>
          )}
        </div>
      )}

      {error && expanded && (
        <div className="auto-chat-error">{error}</div>
      )}

      {showSketchModal && (
        <div className="sketch-modal-overlay" onClick={() => setShowSketchModal(false)}>
          <div className="sketch-modal" onClick={(e) => e.stopPropagation()}>
            <div className="sketch-modal-header">
              <h2>DataHub Sketch</h2>
              <button
                className="sketch-modal-close"
                onClick={() => setShowSketchModal(false)}
              >
                ✕
              </button>
            </div>
            <div className="sketch-modal-body">
              {loadingSketch && <div className="sketch-loading">Loading sketch...</div>}
              {sketchError && <div className="sketch-error">{sketchError}</div>}
              {!loadingSketch && !sketchError && sketch && (
                <JsonViewer data={sketch} />
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
