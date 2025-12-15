/**
 * Utilities for managing conversation state in sessionStorage.
 *
 * These utilities allow components outside the AskDataHubProvider scope
 * to access conversation state without requiring React Context,
 * minimizing changes to shared base components.
 */

const SESSION_STORAGE_KEY = 'askDataHub:sessionId';
let sessionIdCache: string | undefined;

/**
 * Generate or retrieve session ID - stored in sessionStorage.
 * The sessionId is used by AskDataHubProvider to construct storage keys for persisting
 * conversation state. This approach avoids OSS merge conflicts by keeping the provider
 * scoped to just the tab content.
 */
export const getOrCreateSessionId = (): string => {
    if (sessionIdCache) {
        return sessionIdCache;
    }

    try {
        const stored = sessionStorage.getItem(SESSION_STORAGE_KEY);
        if (stored) {
            sessionIdCache = stored;
            return stored;
        }

        const newId = `session_${Date.now()}_${Math.random()}`;
        sessionStorage.setItem(SESSION_STORAGE_KEY, newId);
        sessionIdCache = newId;
        return newId;
    } catch {
        // sessionStorage may be unavailable in private browsing mode or when storage quota is exceeded.
        // Fall back to generating a session ID without persistence - conversation state will still work
        // but won't persist across tab switches within the same session.
        const fallbackId = `session_${Date.now()}_${Math.random()}`;
        sessionIdCache = fallbackId;
        return fallbackId;
    }
};
