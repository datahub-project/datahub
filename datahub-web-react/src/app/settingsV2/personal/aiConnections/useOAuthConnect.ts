import { useCallback, useEffect, useRef, useState } from 'react';

import { notification } from '@src/alchemy-components';

/**
 * Hook for handling OAuth connection flow with popup window.
 *
 * Flow:
 * 1. Call initiateOAuthConnect(pluginId) to start the flow
 * 2. Popup opens synchronously (Safari-safe: about:blank first)
 * 3. Backend returns an authorization URL, popup navigates to it
 * 4. User authenticates with the OAuth provider
 * 5. Provider redirects back to our callback URL
 * 6. Callback page posts a message to this window with the result
 * 7. We close the popup and call onSuccess
 */
export function useOAuthConnect(onSuccess?: () => void) {
    const [connectingPluginId, setConnectingPluginId] = useState<string | null>(null);
    const popupRef = useRef<Window | null>(null);
    const popupCheckRef = useRef<ReturnType<typeof setInterval> | null>(null);

    // For backwards compatibility
    const isConnecting = connectingPluginId !== null;

    // Listen for messages from the OAuth popup
    useEffect(() => {
        const handleMessage = (event: MessageEvent) => {
            const { type, success, error } = event.data || {};

            if (type === 'oauth_callback') {
                setConnectingPluginId(null);

                // Clean up popup check interval
                if (popupCheckRef.current) {
                    clearInterval(popupCheckRef.current);
                    popupCheckRef.current = null;
                }

                // Close the popup
                if (popupRef.current && !popupRef.current.closed) {
                    popupRef.current.close();
                }
                popupRef.current = null;

                if (success) {
                    // Small delay to ensure backend has finished updating before refetch
                    setTimeout(() => {
                        onSuccess?.();
                    }, 500);
                } else {
                    notification.error({
                        message: 'Connection Failed',
                        description: error || 'Failed to connect. Please try again.',
                    });
                }
            }
        };

        window.addEventListener('message', handleMessage);
        return () => window.removeEventListener('message', handleMessage);
    }, [onSuccess]);

    // Clean up on unmount
    useEffect(() => {
        return () => {
            if (popupCheckRef.current) {
                clearInterval(popupCheckRef.current);
                popupCheckRef.current = null;
            }
            if (popupRef.current && !popupRef.current.closed) {
                popupRef.current.close();
            }
        };
    }, []);

    const initiateOAuthConnect = useCallback(async (pluginId: string) => {
        setConnectingPluginId(pluginId);

        // Open popup SYNCHRONOUSLY in the click handler to preserve the user
        // gesture. Safari blocks window.open() if called after an async
        // operation (fetch). We open about:blank first, then navigate.
        const width = 600;
        const height = 700;
        const left = window.screenX + (window.outerWidth - width) / 2;
        const top = window.screenY + (window.outerHeight - height) / 2;

        const popup = window.open(
            'about:blank',
            'oauth_connect',
            `width=${width},height=${height},left=${left},top=${top},toolbar=no,menubar=no,scrollbars=yes,resizable=yes`,
        );

        if (!popup) {
            setConnectingPluginId(null);
            notification.error({
                message: 'Popup Blocked',
                description: 'Please allow popups for this site and try again.',
            });
            return;
        }

        popupRef.current = popup;

        try {
            // Get the authorization URL from the backend
            const response = await fetch(`/integrations/oauth/plugins/${encodeURIComponent(pluginId)}/connect`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
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

            // Navigate the already-open popup to the OAuth provider
            popup.location.href = authorizationUrl;

            // Monitor popup close
            popupCheckRef.current = setInterval(() => {
                if (popupRef.current?.closed) {
                    if (popupCheckRef.current) {
                        clearInterval(popupCheckRef.current);
                        popupCheckRef.current = null;
                    }
                    setConnectingPluginId(null);
                    popupRef.current = null;
                }
            }, 500);
        } catch (error) {
            // Close the blank popup on error
            if (popup && !popup.closed) {
                popup.close();
            }
            popupRef.current = null;
            setConnectingPluginId(null);
            notification.error({
                message: 'Connection Failed',
                description: error instanceof Error ? error.message : 'Failed to connect. Please try again.',
            });
        }
    }, []);

    return {
        initiateOAuthConnect,
        isConnecting,
        connectingPluginId,
    };
}

export default useOAuthConnect;
