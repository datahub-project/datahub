import React, { ReactNode, createContext, useContext, useEffect, useMemo, useState } from 'react';

import { getOrCreateSessionId } from '@app/entityV2/shared/tabs/AskDataHub/utils/conversationStorage';

interface AskDataHubContextType {
    conversationUrn: string | null;
    setConversationUrn: (urn: string | null) => void;
}

const AskDataHubContext = createContext<AskDataHubContextType | undefined>(undefined);

interface AskDataHubProviderProps {
    children: ReactNode;
    entityUrn: string;
}

/**
 * Provider that persists conversation state across tab switches using sessionStorage.
 *
 * WHY needed: When switching sidebar tabs, AskDataHubTab unmounts and would lose state.
 * This provider persists conversationUrn to sessionStorage so conversations survive tab switches
 * but clear on page refresh (session ID changes, making old keys inaccessible).
 *
 * WHY this approach: Provider wraps only the tab (not EntityProfileSidebar) to avoid OSS merge conflicts.
 */
export const AskDataHubProvider: React.FC<AskDataHubProviderProps> = ({ children, entityUrn }) => {
    // Storage key: session ID + entity URN
    // - Session ID isolates data to current page session (clears on refresh)
    // - Entity URN isolates conversations per entity (different chats for different assets)
    const storageKey = `askDataHub:${getOrCreateSessionId()}:${entityUrn}`;

    // Restore conversationUrn from sessionStorage on mount
    const [conversationUrn, setConversationUrn] = useState<string | null>(() => {
        try {
            return sessionStorage.getItem(storageKey);
        } catch {
            return null;
        }
    });

    // Re-read from sessionStorage when entityUrn changes (user navigates to different entity)
    useEffect(() => {
        try {
            const storedUrn = sessionStorage.getItem(storageKey);
            setConversationUrn(storedUrn);
        } catch {
            setConversationUrn(null);
        }
    }, [storageKey]);

    // Persist to sessionStorage whenever conversationUrn changes
    useEffect(() => {
        try {
            if (conversationUrn) {
                sessionStorage.setItem(storageKey, conversationUrn);
            } else {
                sessionStorage.removeItem(storageKey);
            }
        } catch {
            // Ignore sessionStorage errors
        }
    }, [conversationUrn, storageKey]);

    const value = useMemo(() => ({ conversationUrn, setConversationUrn }), [conversationUrn]);

    return <AskDataHubContext.Provider value={value}>{children}</AskDataHubContext.Provider>;
};

export const useAskDataHubContext = () => {
    const context = useContext(AskDataHubContext);
    if (context === undefined) {
        throw new Error('useAskDataHubContext must be used within an AskDataHubProvider');
    }
    return context;
};
