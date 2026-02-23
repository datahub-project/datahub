/**
 * Custom hook for Slack OAuth flow logic.
 *
 * Follows the same pattern as MCP plugin OAuth:
 * 1. POST /integrations/slack/connect  (authenticated via cookie)
 * 2. Backend stores state server-side and returns an authorization URL
 * 3. Frontend redirects the user to that URL
 * 4. Callback redirects back with success/error query params
 *
 * No client-side state generation — the backend handles nonces,
 * user identity, and single-use enforcement.
 */
import { message } from 'antd';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

type UseSlackOAuthOptions = {
    isSlackPlatformConfigured: boolean;
    /** If true, automatically trigger OAuth flow when ?connect_slack=true is in URL */
    autoConnectEnabled?: boolean;
};

type UseSlackOAuthResult = {
    startOAuthFlow: () => void;
    isLoading: boolean;
    handleOAuthRedirect: () => void;
    /** Whether auto-connect was requested via URL param */
    shouldAutoConnect: boolean;
};

/**
 * Hook that manages Slack OAuth flow for personal notifications.
 *
 * Features:
 * - Calls /integrations/slack/connect to get a server-generated OAuth URL
 * - Backend authenticates via PLAY_SESSION cookie (no token needed in JS)
 * - Handles OAuth redirects (success/error)
 * - Auto-connects when ?connect_slack=true is in URL (e.g., from Slack bot link)
 */
export function useSlackOAuth({
    isSlackPlatformConfigured,
    autoConnectEnabled = true,
}: UseSlackOAuthOptions): UseSlackOAuthResult {
    const [isLoading, setIsLoading] = useState(false);
    const hasTriggeredAutoConnect = useRef(false);

    // Check for ?connect_slack=true URL parameter (from Slack bot "Connect your account" link)
    const shouldAutoConnect = useMemo(() => {
        const urlParams = new URLSearchParams(window.location.search);
        return urlParams.get('connect_slack') === 'true';
    }, []);

    /**
     * Start the OAuth flow by calling the connect endpoint and redirecting.
     *
     * Mirrors useOAuthConnect from the MCP plugin flow, but uses a
     * full-page redirect instead of a popup.
     */
    const startOAuthFlow = useCallback(async () => {
        if (!isSlackPlatformConfigured) {
            message.error('Slack platform integration is not properly configured. Please contact your administrator.');
            return;
        }

        setIsLoading(true);
        try {
            const response = await fetch('/integrations/slack/connect', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include', // Include PLAY_SESSION cookie for auth
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

            message.info('Redirecting to Slack for authentication...');
            window.location.href = authorizationUrl;
        } catch (error) {
            setIsLoading(false);
            message.error(
                error instanceof Error ? error.message : 'Failed to load OAuth configuration. Please try again.',
            );
        }
    }, [isSlackPlatformConfigured]);

    /**
     * Handle OAuth redirect parameters in URL.
     * This effect runs on mount and checks for success/error params.
     */
    const handleOAuthRedirect = useCallback(() => {
        const urlParams = new URLSearchParams(window.location.search);
        const oauthStatus = urlParams.get('slack_oauth');
        const slackUserId = urlParams.get('slack_user_id');
        const errorMessage = urlParams.get('message');

        if (oauthStatus === 'success' && slackUserId) {
            // Refresh the page to load updated settings
            window.location.href = window.location.pathname;
        } else if (oauthStatus === 'error' && errorMessage) {
            message.error(`Slack connection failed: ${errorMessage}`);
        }
    }, []);

    // Handle OAuth redirect on mount
    useEffect(() => {
        handleOAuthRedirect();
    }, [handleOAuthRedirect]);

    // Auto-trigger OAuth flow when ?connect_slack=true is in URL
    // This happens when user clicks "Connect your account" link from Slack bot
    useEffect(() => {
        if (
            autoConnectEnabled &&
            shouldAutoConnect &&
            !hasTriggeredAutoConnect.current &&
            !isLoading &&
            isSlackPlatformConfigured
        ) {
            hasTriggeredAutoConnect.current = true;

            // Clean up URL parameter before redirecting
            const newUrl = window.location.pathname;
            window.history.replaceState({}, document.title, newUrl);

            // Small delay to let the message show before redirect
            setTimeout(() => {
                startOAuthFlow();
            }, 500);
        }
    }, [autoConnectEnabled, shouldAutoConnect, isLoading, isSlackPlatformConfigured, startOAuthFlow]);

    return {
        startOAuthFlow,
        isLoading,
        handleOAuthRedirect,
        shouldAutoConnect,
    };
}
