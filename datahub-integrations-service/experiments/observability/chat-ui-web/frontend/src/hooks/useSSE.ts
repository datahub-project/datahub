import { useEffect, useRef, useState } from 'react';

interface UseSSEOptions {
  onToken?: (token: string) => void;
  onDone?: (messageId: string, tokens: number) => void;
  onError?: (error: string) => void;
}

export function useSSE() {
  const [isStreaming, setIsStreaming] = useState(false);
  const eventSourceRef = useRef<EventSource | null>(null);

  const startStream = (
    eventSource: EventSource,
    options: UseSSEOptions = {}
  ) => {
    eventSourceRef.current = eventSource;
    setIsStreaming(true);

    eventSource.addEventListener('token', (event) => {
      try {
        const data = JSON.parse(event.data);
        options.onToken?.(data.token);
      } catch (error) {
        console.error('Error parsing token event:', error);
      }
    });

    eventSource.addEventListener('done', (event) => {
      try {
        const data = JSON.parse(event.data);
        options.onDone?.(data.message_id, data.tokens);
      } catch (error) {
        console.error('Error parsing done event:', error);
      } finally {
        setIsStreaming(false);
        eventSource.close();
      }
    });

    eventSource.addEventListener('error', (event) => {
      try {
        const data = JSON.parse((event as MessageEvent).data);
        options.onError?.(data.error);
      } catch (error) {
        options.onError?.('Stream error occurred');
      } finally {
        setIsStreaming(false);
        eventSource.close();
      }
    });

    eventSource.onerror = () => {
      options.onError?.('Connection error');
      setIsStreaming(false);
      eventSource.close();
    };
  };

  const stopStream = () => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
      setIsStreaming(false);
    }
  };

  useEffect(() => {
    return () => {
      stopStream();
    };
  }, []);

  return { isStreaming, startStream, stopStream };
}
