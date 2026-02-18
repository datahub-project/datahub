import { useCallback, useEffect, useRef, useState } from 'react';

import { PluginFormState } from '@app/settingsV2/platform/ai/plugins/utils/pluginFormState';
import { notification } from '@src/alchemy-components';

type TestResult = {
    success: boolean;
    toolCount?: number;
    toolNames?: string[];
    message?: string;
    error?: string;
};

type TestConnectionState = {
    isTestingConnection: boolean;
    testResult: TestResult | null;
};

/**
 * Hook for testing OAuth + MCP server connectivity.
 *
 * Initiates a transient OAuth flow via the test-oauth-connect endpoint,
 * opens a popup for user consent, and receives the test result via postMessage.
 */
export function useTestOAuthConnection() {
    const [state, setState] = useState<TestConnectionState>({
        isTestingConnection: false,
        testResult: null,
    });
    const popupRef = useRef<Window | null>(null);
    const popupCheckRef = useRef<ReturnType<typeof setInterval> | null>(null);
    const isTestingRef = useRef(false);

    const handleResult = useCallback((result: TestResult) => {
        isTestingRef.current = false;
        setState({ isTestingConnection: false, testResult: result });
        if (popupCheckRef.current) {
            clearInterval(popupCheckRef.current);
            popupCheckRef.current = null;
        }
        if (popupRef.current && !popupRef.current.closed) {
            popupRef.current.close();
        }
        popupRef.current = null;
    }, []);

    // Listen for postMessage from popup
    useEffect(() => {
        const handleMessage = (event: MessageEvent) => {
            const data = event.data || {};
            if (data.type === 'oauth_test_result') {
                handleResult({
                    success: data.success,
                    toolCount: data.toolCount,
                    toolNames: data.toolNames,
                    message: data.message,
                    error: data.error,
                });
            }
        };

        window.addEventListener('message', handleMessage);
        return () => window.removeEventListener('message', handleMessage);
    }, [handleResult]);

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

    const testConnection = useCallback(async (formState: PluginFormState) => {
        isTestingRef.current = true;
        setState({ isTestingConnection: true, testResult: null });

        // Open popup SYNCHRONOUSLY in the click handler to preserve the user
        // gesture. Safari blocks window.open() if it's called after an async
        // operation (fetch). We open a blank page first, then navigate it to
        // the authorization URL after the fetch completes.
        const width = 600;
        const height = 700;
        const left = window.screenX + (window.outerWidth - width) / 2;
        const top = window.screenY + (window.outerHeight - height) / 2;

        const popup = window.open(
            'about:blank',
            'oauth_test_connect',
            `width=${width},height=${height},left=${left},top=${top},toolbar=no,menubar=no,scrollbars=yes,resizable=yes`,
        );

        if (!popup) {
            isTestingRef.current = false;
            setState({ isTestingConnection: false, testResult: null });
            notification.error({
                message: 'Popup Blocked',
                description: 'Please allow popups for this site and try again.',
            });
            return;
        }

        popupRef.current = popup;

        try {
            // Build OAuth config from form state
            const oauthConfig: Record<string, unknown> = {
                clientId: formState.oauthClientId,
                clientSecret: formState.oauthClientSecret,
                authorizationUrl: formState.oauthAuthorizationUrl,
                tokenUrl: formState.oauthTokenUrl,
                scopes: formState.oauthScopes
                    ? formState.oauthScopes
                          .split(',')
                          .map((s) => s.trim())
                          .filter(Boolean)
                    : [],
                tokenAuthMethod: formState.oauthTokenAuthMethod || 'POST_BODY',
                authScheme: formState.oauthAuthScheme,
                authHeaderName: formState.oauthAuthHeaderName,
            };

            // Build MCP config from form state
            const mcpConfig: Record<string, unknown> = {
                url: formState.url,
                transport: formState.transport || 'HTTP',
                timeout: formState.timeout ? parseFloat(formState.timeout) : 30,
                customHeaders: formState.customHeaders?.reduce(
                    (acc, h) => {
                        if (h.key && h.value) acc[h.key] = h.value;
                        return acc;
                    },
                    {} as Record<string, string>,
                ),
            };

            // Fetch the authorization URL from the backend
            const response = await fetch('/integrations/oauth/plugins/test-oauth-connect', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({
                    oauth_config: oauthConfig,
                    mcp_config: mcpConfig,
                }),
            });

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({}));
                throw new Error(errorData.detail || `Test connection failed: ${response.status}`);
            }

            const data = await response.json();
            const { authorization_url: authorizationUrl } = data;

            if (!authorizationUrl) {
                throw new Error('No authorization URL returned from server');
            }

            // Navigate the already-open popup to the OAuth provider
            popup.location.href = authorizationUrl;

            // Monitor popup close -- if user closes popup before completing,
            // reset the testing state after a brief delay for postMessage
            popupCheckRef.current = setInterval(() => {
                if (popupRef.current?.closed) {
                    if (popupCheckRef.current) {
                        clearInterval(popupCheckRef.current);
                        popupCheckRef.current = null;
                    }
                    setTimeout(() => {
                        if (isTestingRef.current) {
                            isTestingRef.current = false;
                            setState({ isTestingConnection: false, testResult: null });
                        }
                    }, 2000);
                }
            }, 500);
        } catch (error) {
            // Close the blank popup on error
            if (popup && !popup.closed) {
                popup.close();
            }
            popupRef.current = null;
            isTestingRef.current = false;
            setState({ isTestingConnection: false, testResult: null });
            notification.error({
                message: 'Test Connection Failed',
                description: error instanceof Error ? error.message : 'Please try again.',
            });
        }
    }, []);

    const clearTestResult = useCallback(() => {
        setState({ isTestingConnection: false, testResult: null });
    }, []);

    return {
        testConnection,
        isTestingConnection: state.isTestingConnection,
        testResult: state.testResult,
        clearTestResult,
    };
}

export default useTestOAuthConnection;
