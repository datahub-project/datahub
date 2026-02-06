import { message } from 'antd';
import { useCallback, useEffect, useRef, useState } from 'react';

/**
 * Hook for handling OAuth connection flow with popup window.
 *
 * Flow:
 * 1. Call initiateOAuthConnect(pluginId) to start the flow
 * 2. Backend returns an authorization URL
 * 3. Open a popup window to that URL
 * 4. User authenticates with the OAuth provider
 * 5. Provider redirects back to our callback URL
 * 6. Callback page posts a message to this window with the result
 * 7. We close the popup and refetch data
 */
export function useOAuthConnect(onSuccess?: () => void) {
    const [connectingPluginId, setConnectingPluginId] = useState<string | null>(null);
    const popupRef = useRef<Window | null>(null);

    // For backwards compatibility
    const isConnecting = connectingPluginId !== null;

    // Listen for messages from the OAuth popup
    useEffect(() => {
        const handleMessage = (event: MessageEvent) => {
            // The backend sends postMessage with '*' origin, so we can't strictly verify origin
            // Instead, we verify the message structure
            const { type, success, pluginId, error } = event.data || {};

            // Debug logging
            console.log('[OAuth] Received postMessage:', { type, success, pluginId, error, origin: event.origin });

            // Backend sends 'oauth_callback' (lowercase)
            if (type === 'oauth_callback') {
                console.log('[OAuth] Processing oauth_callback message');
                setConnectingPluginId(null);

                // Close the popup
                if (popupRef.current && !popupRef.current.closed) {
                    popupRef.current.close();
                }
                popupRef.current = null;

                if (success) {
                    console.log('[OAuth] Connection successful, triggering refetch');
                    // Small delay to ensure backend has finished updating before refetch
                    setTimeout(() => {
                        console.log('[OAuth] Calling onSuccess/refetch');
                        onSuccess?.();
                    }, 500);
                } else {
                    message.error(error || 'Failed to connect. Please try again.');
                }
            }
        };

        window.addEventListener('message', handleMessage);
        return () => window.removeEventListener('message', handleMessage);
    }, [onSuccess]);

    // Clean up popup on unmount
    useEffect(() => {
        return () => {
            if (popupRef.current && !popupRef.current.closed) {
                popupRef.current.close();
            }
        };
    }, []);

    const initiateOAuthConnect = useCallback(async (pluginId: string) => {
        setConnectingPluginId(pluginId);

        try {
            // Get the authorization URL from the backend
            const response = await fetch(`/integrations/oauth/plugins/${encodeURIComponent(pluginId)}/connect`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                credentials: 'include', // Include cookies for auth
            });

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({}));
                throw new Error(errorData.detail || `Failed to initiate OAuth: ${response.status}`);
            }

            const data = await response.json();
            const { authorization_url: authorizationUrl } = data;

            if (!authorizationUrl) {
                throw new Error('No authorization URL returned from server');
            }

            // Calculate popup position (centered)
            const width = 600;
            const height = 700;
            const left = window.screenX + (window.outerWidth - width) / 2;
            const top = window.screenY + (window.outerHeight - height) / 2;

            // Open the popup
            popupRef.current = window.open(
                authorizationUrl,
                'oauth_connect',
                `width=${width},height=${height},left=${left},top=${top},toolbar=no,menubar=no,scrollbars=yes,resizable=yes`,
            );

            if (!popupRef.current) {
                throw new Error('Failed to open popup. Please allow popups for this site.');
            }

            // Check if popup was closed without completing
            const checkPopupClosed = setInterval(() => {
                if (popupRef.current?.closed) {
                    clearInterval(checkPopupClosed);
                    setConnectingPluginId(null);
                    popupRef.current = null;
                }
            }, 500);
        } catch (error) {
            setConnectingPluginId(null);
            message.error(error instanceof Error ? error.message : 'Failed to connect. Please try again.');
        }
    }, []);

    return {
        initiateOAuthConnect,
        isConnecting,
        connectingPluginId,
    };
}

export default useOAuthConnect;
